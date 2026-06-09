#include "postgres.h"
#include "fmgr.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/parallel.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "commands/vacuum.h"
#include "access/valid.h"
#include "catalog/pg_index.h"
#include "catalog/pg_type_d.h"

#include "treedb_pgext.h"

PG_FUNCTION_INFO_V1(treedb_am_handler);
/* forward decl for handler log */
Datum treedb_am_handler(PG_FUNCTION_ARGS);

/* ----------------------------------------------------------------
 * Scan descriptor (embedded TableScanDescData must be first).
 * ---------------------------------------------------------------- */
typedef struct TDBScanDesc
{
    TableScanDescData   rs_base;        /* must be first */
    uint64              scan_id;        /* scan ID assigned by the background worker */
    bool                scan_done;      /* true once SCAN_NEXT_BATCH returned NOT_FOUND */
    /* Batch prefetch state */
    uint8              *batch_buf;      /* palloc'd, TDB_SCAN_RESP_BUF bytes */
    uint32              batch_nrows;    /* rows remaining in buffer */
    uint32              batch_pos;      /* byte offset of next row in batch_buf */
} TDBScanDesc;

/* ----------------------------------------------------------------
 * Index-fetch state.
 * ---------------------------------------------------------------- */
typedef struct TDBIndexFetchData
{
    IndexFetchTableData base; /* must be first */
} TDBIndexFetchData;

/* ----------------------------------------------------------------
 * Helper: reconstruct a palloc'd HeapTuple from raw t_data bytes.
 * ---------------------------------------------------------------- */
static HeapTuple
tdb_make_tuple(Oid tableOid, ItemPointer tid, const char *data, uint32 data_len)
{
    HeapTuple tuple = (HeapTuple) palloc(HEAPTUPLESIZE + data_len);

    tuple->t_len     = data_len;
    tuple->t_self    = *tid;
    tuple->t_tableOid = tableOid;
    tuple->t_data    = (HeapTupleHeader)((char *) tuple + HEAPTUPLESIZE);
    memcpy(tuple->t_data, data, data_len);
    return tuple;
}

/* ----------------------------------------------------------------
 * High-level IPC wrappers
 * ---------------------------------------------------------------- */

static void
tdb_insert_rpc(RelFileNumber relnum, const char *tuple_data, uint32 tuple_len,
               uint64 *seq_out)
{
    uint32  req_len = 4 + 4 + tuple_len;
    uint8  *req     = (uint8 *) palloc(req_len);
    void   *resp    = NULL;
    uint32  resp_len;

    tdb_put_u32(req + 0, (uint32) relnum);
    tdb_put_u32(req + 4, tuple_len);
    memcpy(req + 8, tuple_data, tuple_len);

    tdb_rpc(TDB_OP_INSERT, req, req_len, &resp, &resp_len);
    pfree(req);

    if (resp && resp_len >= 8)
        *seq_out = tdb_get_u64((uint8 *) resp);
    else
        *seq_out = 0;
    if (resp) pfree(resp);
}

static uint64
tdb_scan_begin_rpc(RelFileNumber relnum)
{
    uint8   req[4];
    void   *resp    = NULL;
    uint32  resp_len;
    uint64  scan_id = 0;

    tdb_put_u32(req, (uint32) relnum);
    tdb_rpc(TDB_OP_SCAN_BEGIN, req, 4, &resp, &resp_len);

    if (resp && resp_len >= 8)
        scan_id = tdb_get_u64((uint8 *) resp);
    if (resp) pfree(resp);
    return scan_id;
}

/*
 * Fetch the next tuple from a scan, using batch prefetch.
 *
 * When the local prefetch buffer is empty, issues a SCAN_NEXT_BATCH RPC
 * to refill it with up to TDB_SCAN_BATCH_BUF bytes of rows.  Subsequent
 * calls drain the buffer without any IPC until it empties again.
 *
 * Batch response layout: [num_rows(4)][seq(8) len(4) data...] × N
 *
 * Returns true if a tuple was placed in slot, false if scan is exhausted.
 */
static bool
tdb_scan_next_batch(TDBScanDesc *scan, Oid tableOid, TupleTableSlot *slot)
{
    uint8           req[12];
    uint32          resp_len;
    uint8           status;
    uint32          nrows;
    uint64          seq_num;
    uint32          tuple_len;
    ItemPointerData tid;
    HeapTuple       tuple;
    uint8          *p;

    /* Refill when buffer is drained. */
    if (scan->batch_nrows == 0)
    {
        tdb_put_u64(req, scan->scan_id);
        tdb_put_u32(req + 8, (uint32) TDB_SCAN_BATCH_BUF);
        status = tdb_rpc_into(TDB_OP_SCAN_NEXT_BATCH, req, 12,
                              scan->batch_buf, TDB_SCAN_RESP_BUF, &resp_len);

        if (status == TDB_STATUS_NOT_FOUND)
            return false;

        if (resp_len < 4)
            ereport(ERROR, (errmsg("treedb: malformed SCAN_NEXT_BATCH response")));

        nrows = tdb_get_u32(scan->batch_buf);
        if (nrows == 0)
            return false;

        scan->batch_nrows = nrows;
        scan->batch_pos   = 4; /* skip the leading num_rows field */
    }

    /* Consume one row from the buffer. */
    p         = scan->batch_buf + scan->batch_pos;
    seq_num   = tdb_get_u64(p);      p += 8;
    tuple_len = tdb_get_u32(p);      p += 4;

    tdb_seq_to_ctid(seq_num, &tid);
    tuple = tdb_make_tuple(tableOid, &tid, (char *) p, tuple_len);

    scan->batch_pos   = (uint32)(p + tuple_len - scan->batch_buf);
    scan->batch_nrows--;

    ExecStoreHeapTuple(tuple, slot, true);
    return true;
}

static void
tdb_scan_end_rpc(uint64 scan_id)
{
    uint8 req[8];
    tdb_put_u64(req, scan_id);
    tdb_rpc(TDB_OP_SCAN_END, req, 8, NULL, NULL);
}

static bool
tdb_fetch_by_seq(RelFileNumber relnum, uint64 seq_num,
                 Oid tableOid, TupleTableSlot *slot)
{
    uint8   req[12];
    void   *resp    = NULL;
    uint32  resp_len;
    uint8   status;
    ItemPointerData tid;
    HeapTuple tuple;

    tdb_put_u32(req + 0, (uint32) relnum);
    tdb_put_u64(req + 4, seq_num);

    status = tdb_rpc(TDB_OP_FETCH, req, 12, &resp, &resp_len);
    if (status == TDB_STATUS_NOT_FOUND)
    {
        if (resp) pfree(resp);
        return false;
    }

    tdb_seq_to_ctid(seq_num, &tid);
    tuple = tdb_make_tuple(tableOid, &tid, (char *) resp, resp_len);
    pfree(resp);

    ExecStoreHeapTuple(tuple, slot, true);
    return true;
}

static void
tdb_delete_rpc(RelFileNumber relnum, uint64 seq_num)
{
    uint8 req[12];
    tdb_put_u32(req + 0, (uint32) relnum);
    tdb_put_u64(req + 4, seq_num);
    tdb_rpc(TDB_OP_DELETE, req, 12, NULL, NULL);
}

/*
 * Insert at a caller-supplied key (used when the PK is known at insert time).
 * Returns the key unchanged (mirrors tdb_insert_rpc's seq_out convention).
 */
static void
tdb_insert_keyed_rpc(RelFileNumber relnum, uint64 key,
                     const char *tuple_data, uint32 tuple_len)
{
    uint32  req_len = 4 + 8 + 4 + tuple_len;
    uint8  *req     = (uint8 *) palloc(req_len);

    tdb_put_u32(req + 0,  (uint32) relnum);
    tdb_put_u64(req + 4,  key);
    tdb_put_u32(req + 12, tuple_len);
    memcpy(req + 16, tuple_data, tuple_len);

    tdb_rpc(TDB_OP_INSERT_KEYED, req, req_len, NULL, NULL);
    pfree(req);
}

/*
 * Rename a key in TreeDB: copy the value from old_key to new_key then delete
 * old_key.  Used during primary-key index build to rekey rows that were
 * inserted before the PK was defined (seq_num → pk_value).
 */
static void
tdb_rekey_rpc(RelFileNumber relnum, uint64 old_key, uint64 new_key)
{
    uint8 req[20];
    tdb_put_u32(req + 0,  (uint32) relnum);
    tdb_put_u64(req + 4,  old_key);
    tdb_put_u64(req + 12, new_key);
    tdb_rpc(TDB_OP_REKEY, req, 20, NULL, NULL);
}

static void
tdb_update_rpc(RelFileNumber relnum, uint64 old_seq,
               const char *tuple_data, uint32 tuple_len,
               uint64 *seq_out)
{
    uint32  req_len = 4 + 8 + 4 + tuple_len;
    uint8  *req     = (uint8 *) palloc(req_len);
    void   *resp    = NULL;
    uint32  resp_len;

    tdb_put_u32(req + 0,  (uint32) relnum);
    tdb_put_u64(req + 4,  old_seq);
    tdb_put_u32(req + 12, tuple_len);
    memcpy(req + 16, tuple_data, tuple_len);

    tdb_rpc(TDB_OP_UPDATE, req, req_len, &resp, &resp_len);
    pfree(req);

    if (resp && resp_len >= 8)
        *seq_out = tdb_get_u64((uint8 *) resp);
    else
        *seq_out = old_seq;
    if (resp) pfree(resp);
}


/* ----------------------------------------------------------------
 * slot_callbacks
 * ---------------------------------------------------------------- */
static const TupleTableSlotOps *
treedb_slot_callbacks(Relation rel)
{
    return &TTSOpsHeapTuple;
}

/* ----------------------------------------------------------------
 * Scan callbacks
 * ---------------------------------------------------------------- */
static TableScanDesc
treedb_scan_begin(Relation rel, Snapshot snapshot, int nkeys,
                  struct ScanKeyData *key, ParallelTableScanDesc pscan,
                  uint32 flags)
{
    TDBScanDesc *scan = (TDBScanDesc *) palloc0(sizeof(TDBScanDesc));

    scan->rs_base.rs_rd       = rel;
    scan->rs_base.rs_snapshot = snapshot;
    scan->rs_base.rs_nkeys    = nkeys;
    scan->rs_base.rs_flags    = flags;
    scan->scan_done           = false;
    scan->batch_buf           = (uint8 *) palloc(TDB_SCAN_RESP_BUF);
    scan->batch_nrows         = 0;
    scan->batch_pos           = 0;
    scan->scan_id             = tdb_scan_begin_rpc(rel->rd_locator.relNumber);

    return (TableScanDesc) scan;
}

static void
treedb_scan_end(TableScanDesc sscan)
{
    TDBScanDesc *scan = (TDBScanDesc *) sscan;

    if (!scan->scan_done)
        tdb_scan_end_rpc(scan->scan_id);

    if (sscan->rs_flags & SO_TEMP_SNAPSHOT)
        UnregisterSnapshot(sscan->rs_snapshot);

    pfree(scan);
}

static void
treedb_scan_rescan(TableScanDesc sscan, struct ScanKeyData *key,
                   bool set_params, bool allow_strat,
                   bool allow_sync, bool allow_pagemode)
{
    TDBScanDesc *scan = (TDBScanDesc *) sscan;

    /* End the existing scan and start a new one. */
    if (!scan->scan_done)
        tdb_scan_end_rpc(scan->scan_id);

    scan->scan_done   = false;
    scan->batch_nrows = 0;
    scan->batch_pos   = 0;
    scan->scan_id     = tdb_scan_begin_rpc(sscan->rs_rd->rd_locator.relNumber);
}

static bool
treedb_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
                        TupleTableSlot *slot)
{
    TDBScanDesc *scan = (TDBScanDesc *) sscan;
    bool         found;

    if (scan->scan_done)
    {
        ExecClearTuple(slot);
        return false;
    }

    /* Phase 0: forward scan only. */
    if (direction == BackwardScanDirection)
        ereport(ERROR, (errmsg("treedb: backward scan not supported in Phase 0")));

    found = tdb_scan_next_batch(scan, sscan->rs_rd->rd_id, slot);
    if (!found)
    {
        scan->scan_done = true;
        tdb_scan_end_rpc(scan->scan_id);
        ExecClearTuple(slot);
    }
    return found;
}

/* ----------------------------------------------------------------
 * Parallel scan (stubs — parallel query not supported in Phase 0)
 * ---------------------------------------------------------------- */
static Size
treedb_parallelscan_estimate(Relation rel)
{
    return sizeof(ParallelTableScanDescData);
}

static Size
treedb_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
    return sizeof(ParallelTableScanDescData);
}

static void
treedb_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
}

/* ----------------------------------------------------------------
 * Index scan callbacks
 * ---------------------------------------------------------------- */
static struct IndexFetchTableData *
treedb_index_fetch_begin(Relation rel)
{
    TDBIndexFetchData *data = palloc0(sizeof(TDBIndexFetchData));
    data->base.rel = rel;
    return (struct IndexFetchTableData *) data;
}

static void
treedb_index_fetch_reset(struct IndexFetchTableData *data)
{
}

static void
treedb_index_fetch_end(struct IndexFetchTableData *data)
{
    pfree(data);
}

static bool
treedb_index_fetch_tuple(struct IndexFetchTableData *idata,
                         ItemPointer tid, Snapshot snapshot,
                         TupleTableSlot *slot,
                         bool *call_again, bool *all_dead)
{
    Relation rel = idata->rel;
    uint64   seq = tdb_ctid_to_seq(tid);

    *call_again = false;
    if (all_dead) *all_dead = false;

    return tdb_fetch_by_seq(rel->rd_locator.relNumber, seq, rel->rd_id, slot);
}

/* ----------------------------------------------------------------
 * Non-modifying tuple operations
 * ---------------------------------------------------------------- */
static bool
treedb_tuple_fetch_row_version(Relation rel, ItemPointer tid,
                                Snapshot snapshot, TupleTableSlot *slot)
{
    uint64 seq = tdb_ctid_to_seq(tid);
    return tdb_fetch_by_seq(rel->rd_locator.relNumber, seq, rel->rd_id, slot);
}

static bool
treedb_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
    return ItemPointerIsValid(tid);
}

static void
treedb_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
    /* No versioning in Phase 0: tid is always the latest. */
}

static bool
treedb_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
                                 Snapshot snapshot)
{
    /* Phase 0: all stored tuples are visible. */
    return true;
}

static TransactionId
treedb_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
    /* Phase 0: no dead tuple tracking. */
    return InvalidTransactionId;
}

/* ----------------------------------------------------------------
 * DML callbacks
 * ---------------------------------------------------------------- */
static void
treedb_tuple_insert(Relation rel, TupleTableSlot *slot,
                    CommandId cid, int options,
                    struct BulkInsertStateData *bistate)
{
    bool       shouldFree;
    HeapTuple  tuple;
    uint64     seq_num;

    tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

    /*
     * If the relation already has a single-column int4 primary key, use the
     * PK value as the TreeDB key so ctid = f(pk_value) — deterministic and
     * stable.  This eliminates B-tree index maintenance on UPDATE (TU_None
     * continues to work since the ctid never changes after INSERT).
     *
     * If the PK does not exist yet (e.g. during pgbench -i which inserts rows
     * before creating the primary key), fall through to the seq_num path.
     * treedb_index_build_range_scan will rekey the rows when the PK is built.
     */
    if (OidIsValid(rel->rd_pkindex))
    {
        Relation          pkRel  = relation_open(rel->rd_pkindex, AccessShareLock);
        AttrNumber        pkAttr = pkRel->rd_index->indkey.values[0];
        Form_pg_attribute pkDesc = TupleDescAttr(rel->rd_att, pkAttr - 1);
        bool              is_int4_pk = (pkRel->rd_index->indnatts >= 1 &&
                                        pkDesc->atttypid == INT4OID);
        relation_close(pkRel, AccessShareLock);

        if (is_int4_pk)
        {
            bool  isnull;
            Datum d = heap_getattr(tuple, pkAttr, rel->rd_att, &isnull);
            if (!isnull)
            {
                seq_num = (uint64)(uint32) DatumGetInt32(d);
                tdb_insert_keyed_rpc(rel->rd_locator.relNumber, seq_num,
                                     (const char *) tuple->t_data, tuple->t_len);
                tdb_seq_to_ctid(seq_num, &slot->tts_tid);
                slot->tts_flags &= ~TTS_FLAG_EMPTY;
                if (shouldFree)
                    heap_freetuple(tuple);
                return;
            }
        }
    }

    /* Fallback: let the shim generate a monotonic seq_num. */
    tdb_insert_rpc(rel->rd_locator.relNumber,
                   (const char *) tuple->t_data, tuple->t_len,
                   &seq_num);

    tdb_seq_to_ctid(seq_num, &slot->tts_tid);
    slot->tts_flags &= ~TTS_FLAG_EMPTY;

    if (shouldFree)
        heap_freetuple(tuple);
}

static void
treedb_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
                                 CommandId cid, int options,
                                 struct BulkInsertStateData *bistate,
                                 uint32 specToken)
{
    /* Phase 0: treat as a regular insert. */
    treedb_tuple_insert(rel, slot, cid, options, bistate);
}

static void
treedb_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
                                   uint32 specToken, bool succeeded)
{
    if (!succeeded)
        tdb_delete_rpc(rel->rd_locator.relNumber,
                       tdb_ctid_to_seq(&slot->tts_tid));
}

static void
treedb_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
                    CommandId cid, int options,
                    struct BulkInsertStateData *bistate)
{
    for (int i = 0; i < nslots; i++)
        treedb_tuple_insert(rel, slots[i], cid, options, bistate);
}

static TM_Result
treedb_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
                    Snapshot snapshot, Snapshot crosscheck, bool wait,
                    TM_FailureData *tmfd, bool changingPart)
{
    tdb_delete_rpc(rel->rd_locator.relNumber, tdb_ctid_to_seq(tid));
    return TM_Ok;
}

static TM_Result
treedb_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
                    CommandId cid, Snapshot snapshot, Snapshot crosscheck,
                    bool wait, TM_FailureData *tmfd,
                    LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
    bool      shouldFree;
    HeapTuple tuple;
    uint64    seq;

    tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

    tdb_update_rpc(rel->rd_locator.relNumber,
                   tdb_ctid_to_seq(otid),
                   (const char *) tuple->t_data, tuple->t_len,
                   &seq);

    /* ctid is unchanged (same seq_num) — no index updates needed. */
    tdb_seq_to_ctid(seq, &slot->tts_tid);
    slot->tts_flags &= ~TTS_FLAG_EMPTY;

    if (shouldFree)
        heap_freetuple(tuple);

    if (update_indexes) *update_indexes = TU_None;
    return TM_Ok;
}

static TM_Result
treedb_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
                  TupleTableSlot *slot, CommandId cid,
                  LockTupleMode mode, LockWaitPolicy wait_policy,
                  uint8 flags, TM_FailureData *tmfd)
{
    /* If the slot already holds this tuple (e.g. from a preceding
     * index_fetch_tuple), reuse it — no row-level locking in Phase 0. */
    if (!TupIsNull(slot) && ItemPointerEquals(&slot->tts_tid, tid))
        return TM_Ok;

    if (!tdb_fetch_by_seq(rel->rd_locator.relNumber,
                          tdb_ctid_to_seq(tid), rel->rd_id, slot))
        return TM_Invisible;
    return TM_Ok;
}

/* ----------------------------------------------------------------
 * DDL callbacks
 * ---------------------------------------------------------------- */
static void
treedb_relation_set_new_filelocator(Relation rel,
                                     const RelFileLocator *newrlocator,
                                     char persistence,
                                     TransactionId *freezeXid,
                                     MultiXactId *minmulti)
{
    elog(LOG, "treedb_relation_set_new_filelocator: old relnum=%u new relnum=%u",
         (unsigned)rel->rd_locator.relNumber, (unsigned)newrlocator->relNumber);

    /*
     * Create the physical storage file.  PostgreSQL requires this even for
     * custom TAMs — without it, smgr and WAL code will fail when they try
     * to reference the relation's storage.  The heap AM does the same thing.
     */
    RelationCreateStorage(*newrlocator, persistence, true);

    *freezeXid = InvalidTransactionId;
    *minmulti  = InvalidMultiXactId;
}

static void
treedb_relation_nontransactional_truncate(Relation rel)
{
    tdb_truncate_rpc(rel->rd_locator.relNumber);
}

static void
treedb_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
    ereport(ERROR, (errmsg("treedb: COPY DATA not supported in Phase 0")));
}

static void
treedb_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
                                   Relation OldIndex, bool use_sort,
                                   TransactionId OldestXmin,
                                   TransactionId *xid_cutoff,
                                   MultiXactId *multi_cutoff,
                                   double *num_tuples, double *tups_vacuumed,
                                   double *tups_recently_dead)
{
    ereport(ERROR, (errmsg("treedb: CLUSTER not supported in Phase 0")));
}

static void
treedb_relation_vacuum(Relation rel, struct VacuumParams *params,
                        BufferAccessStrategy bstrategy)
{
}

/* ----------------------------------------------------------------
 * ANALYZE support (minimal — report no blocks so ANALYZE skips us)
 * ---------------------------------------------------------------- */
static bool
treedb_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
    return false;
}

static bool
treedb_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                                double *liverows, double *deadrows,
                                TupleTableSlot *slot)
{
    return false;
}

/* ----------------------------------------------------------------
 * Index build / validate (Phase 0: sequential scan path)
 * ---------------------------------------------------------------- */
static double
treedb_index_build_range_scan(Relation table_rel, Relation index_rel,
                               struct IndexInfo *index_info,
                               bool allow_sync, bool anyvisible, bool progress,
                               BlockNumber start_blockno, BlockNumber numblocks,
                               IndexBuildCallback callback, void *callback_state,
                               TableScanDesc scan)
{
    TupleTableSlot   *slot;
    TableScanDesc     myscan;
    double            ntuples      = 0;
    bool              do_rekey     = false;
    AttrNumber        pk_attr      = 0;
    Form_pg_attribute pk_attr_desc = NULL;

    /*
     * Parallel workers in a parallel index build would each do a full scan
     * since we don't implement parallel scan range coordination.  Skip the
     * scan entirely in worker processes; only the leader (coordinator) scans.
     */
    if (IsParallelWorker())
        return 0;

    /*
     * Detect a single-column int4 primary key so we can rekey rows that were
     * inserted before the PK was defined (seq_num → pk_value).  After rekey
     * the ctid encodes the pk_value, making future FETCHes a direct db.Get
     * without a seq_num indirection.
     */
    if (index_rel->rd_index->indisprimary &&
        index_info->ii_NumIndexKeyAttrs == 1)
    {
        pk_attr      = index_info->ii_IndexAttrNumbers[0];
        pk_attr_desc = TupleDescAttr(table_rel->rd_att, pk_attr - 1);
        if (pk_attr_desc->atttypid == INT4OID)
            do_rekey = true;
    }

    slot = table_slot_create(table_rel, NULL);

    if (scan)
        myscan = scan;
    else
        myscan = table_beginscan(table_rel, SnapshotAny, 0, NULL);

    while (table_scan_getnextslot(myscan, ForwardScanDirection, slot))
    {
        bool        isnull[INDEX_MAX_KEYS];
        Datum       values[INDEX_MAX_KEYS];

        if (do_rekey)
        {
            bool   pk_isnull;
            Datum  d      = slot_getattr(slot, pk_attr, &pk_isnull);
            if (!pk_isnull)
            {
                uint64 pk_val  = (uint64)(uint32) DatumGetInt32(d);
                uint64 old_seq = tdb_ctid_to_seq(&slot->tts_tid);
                if (old_seq != pk_val)
                    tdb_rekey_rpc(table_rel->rd_locator.relNumber, old_seq, pk_val);
                tdb_seq_to_ctid(pk_val, &slot->tts_tid);
            }
        }

        FormIndexDatum(index_info, slot, NULL, values, isnull);
        callback(index_rel, &slot->tts_tid, values, isnull, true, callback_state);
        ntuples++;
    }

    if (!scan)
        table_endscan(myscan);

    ExecDropSingleTupleTableSlot(slot);
    return ntuples;
}

static void
treedb_index_validate_scan(Relation table_rel, Relation index_rel,
                            struct IndexInfo *index_info, Snapshot snapshot,
                            struct ValidateIndexState *state)
{
    /* Phase 0: no-op. */
}

/* ----------------------------------------------------------------
 * Miscellaneous
 * ---------------------------------------------------------------- */
static uint64
treedb_relation_size(Relation rel, ForkNumber forkNumber)
{
    return 0;
}

static bool
treedb_relation_needs_toast_table(Relation rel)
{
    return false; /* Phase 0: no TOAST support */
}

static void
treedb_relation_estimate_size(Relation rel, int32 *attr_widths,
                               BlockNumber *pages, double *tuples,
                               double *allvisfrac)
{
    uint8   req[4];
    void   *resp    = NULL;
    uint32  resp_len;
    double  ntuples = 100.0;

    tdb_put_u32(req, (uint32) rel->rd_locator.relNumber);
    if (tdb_rpc(TDB_OP_COUNT, req, 4, &resp, &resp_len) == TDB_STATUS_OK
        && resp && resp_len >= 8)
    {
        ntuples = (double) tdb_get_u64((uint8 *) resp);
        if (ntuples < 1) ntuples = 1;
    }
    if (resp) pfree(resp);

    *tuples     = ntuples;
    *pages      = (BlockNumber) ((ntuples * 128) / BLCKSZ + 1);
    *allvisfrac = 1.0;
}

/* ----------------------------------------------------------------
 * Sample scan (unsupported in Phase 0)
 * ---------------------------------------------------------------- */
static bool
treedb_scan_sample_next_block(TableScanDesc scan,
                               struct SampleScanState *scanstate)
{
    return false;
}

static bool
treedb_scan_sample_next_tuple(TableScanDesc scan,
                               struct SampleScanState *scanstate,
                               TupleTableSlot *slot)
{
    return false;
}

/* ----------------------------------------------------------------
 * AM handler entry point
 *
 * PostgreSQL expects the returned TableAmRoutine to be statically allocated
 * (see GetTableAmRoutine comment: "which we expect to be statically
 * allocated").  Using makeNode/palloc allocates in CurrentMemoryContext,
 * which is a per-transaction context freed when CREATE TABLE commits —
 * leaving a dangling rd_tableam pointer that crashes on the next INSERT.
 * The heap AM avoids this by returning &heapam_methods (a static struct);
 * we do the same here.
 * ---------------------------------------------------------------- */
static const TableAmRoutine treedb_methods = {
    .type = T_TableAmRoutine,

    .slot_callbacks              = treedb_slot_callbacks,

    .scan_begin                  = treedb_scan_begin,
    .scan_end                    = treedb_scan_end,
    .scan_rescan                 = treedb_scan_rescan,
    .scan_getnextslot            = treedb_scan_getnextslot,

    .scan_set_tidrange           = NULL,
    .scan_getnextslot_tidrange   = NULL,

    .parallelscan_estimate       = treedb_parallelscan_estimate,
    .parallelscan_initialize     = treedb_parallelscan_initialize,
    .parallelscan_reinitialize   = treedb_parallelscan_reinitialize,

    .index_fetch_begin           = treedb_index_fetch_begin,
    .index_fetch_reset           = treedb_index_fetch_reset,
    .index_fetch_end             = treedb_index_fetch_end,
    .index_fetch_tuple           = treedb_index_fetch_tuple,

    .tuple_fetch_row_version     = treedb_tuple_fetch_row_version,
    .tuple_tid_valid             = treedb_tuple_tid_valid,
    .tuple_get_latest_tid        = treedb_tuple_get_latest_tid,
    .tuple_satisfies_snapshot    = treedb_tuple_satisfies_snapshot,
    .index_delete_tuples         = treedb_index_delete_tuples,

    .tuple_insert                = treedb_tuple_insert,
    .tuple_insert_speculative    = treedb_tuple_insert_speculative,
    .tuple_complete_speculative  = treedb_tuple_complete_speculative,
    .multi_insert                = treedb_multi_insert,
    .tuple_delete                = treedb_tuple_delete,
    .tuple_update                = treedb_tuple_update,
    .tuple_lock                  = treedb_tuple_lock,
    .finish_bulk_insert          = NULL,

    .relation_set_new_filelocator       = treedb_relation_set_new_filelocator,
    .relation_nontransactional_truncate = treedb_relation_nontransactional_truncate,
    .relation_copy_data                 = treedb_relation_copy_data,
    .relation_copy_for_cluster          = treedb_relation_copy_for_cluster,
    .relation_vacuum                    = treedb_relation_vacuum,
    .scan_analyze_next_block            = treedb_scan_analyze_next_block,
    .scan_analyze_next_tuple            = treedb_scan_analyze_next_tuple,
    .index_build_range_scan             = treedb_index_build_range_scan,
    .index_validate_scan                = treedb_index_validate_scan,

    .relation_size                      = treedb_relation_size,
    .relation_needs_toast_table         = treedb_relation_needs_toast_table,
    .relation_toast_am                  = NULL,
    .relation_fetch_toast_slice         = NULL,

    .relation_estimate_size             = treedb_relation_estimate_size,

    .scan_bitmap_next_tuple             = NULL,
    .scan_sample_next_block             = treedb_scan_sample_next_block,
    .scan_sample_next_tuple             = treedb_scan_sample_next_tuple,
};

Datum
treedb_am_handler(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(&treedb_methods);
}
