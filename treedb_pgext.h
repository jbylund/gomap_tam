#ifndef TREEDB_PGEXT_H
#define TREEDB_PGEXT_H

#include <stdint.h>
#include <string.h>
#include <sched.h>   /* sched_yield() */

#include "postgres.h"
#include "miscadmin.h"   /* DataDir */
#include "storage/pg_shmem.h"

/* iceoryx2 C bindings */
#include "iox2/iceoryx2.h"

/* ----------------------------------------------------------------
 * Protocol constants (must match treedb_shim.go)
 * ---------------------------------------------------------------- */
#define TDB_OP_INSERT      0x01
#define TDB_OP_SCAN_BEGIN  0x02
#define TDB_OP_SCAN_NEXT   0x03
#define TDB_OP_SCAN_END    0x04
#define TDB_OP_FETCH       0x05
#define TDB_OP_DELETE      0x06
#define TDB_OP_TRUNCATE    0x07
#define TDB_OP_COUNT       0x08
#define TDB_OP_UPDATE        0x09
#define TDB_OP_INSERT_KEYED  0x0A  /* insert at caller-supplied key */
#define TDB_OP_REKEY         0x0B  /* rename old_key → new_key (used during PK index build) */

#define TDB_STATUS_OK        0x00
#define TDB_STATUS_NOT_FOUND 0x01
#define TDB_STATUS_ERROR     0x02

/* Maximum tuple size in a single RPC. */
#define TDB_MAX_TUPLE_BYTES  (8 * 1024)

/* iceoryx2 service name — must match treedb_bgworker.c */
#define TDB_SERVICE_NAME     "treedb/bgworker"

/* Initial max slice length hint for the client (u8 elements). */
#define TDB_CLIENT_MAX_SLICE  (8 * 1024 + 64)

/* ----------------------------------------------------------------
 * Path helpers
 * ---------------------------------------------------------------- */
static inline void
tdb_db_path(char *buf, size_t bufsz)
{
    snprintf(buf, bufsz, "%s/treedb_data", DataDir);
}

/* ----------------------------------------------------------------
 * Per-process iceoryx2 client state.
 *
 * Each backend gets its own iceoryx2 node and client, initialised
 * lazily on the first tdb_rpc() call and reused thereafter.
 * ---------------------------------------------------------------- */
static iox2_node_h  tdb_iox2_node   = NULL;
static iox2_client_h tdb_iox2_client = NULL;

static inline void
tdb_iox2_reset(void)
{
    if (tdb_iox2_client != NULL)
    {
        iox2_client_drop(tdb_iox2_client);
        tdb_iox2_client = NULL;
    }
    if (tdb_iox2_node != NULL)
    {
        iox2_node_drop(tdb_iox2_node);
        tdb_iox2_node = NULL;
    }
}

/* Connect to the iceoryx2 service, retrying for up to 10 s
 * while the background worker is starting up. */
static inline void
tdb_iox2_connect(void)
{
    iox2_node_builder_h                      nb          = NULL;
    iox2_service_name_h                      svc_name    = NULL;
    iox2_service_builder_h                   svc_builder = NULL;
    iox2_service_builder_request_response_h  sb_rr;
    iox2_port_factory_request_response_h     service     = NULL;
    iox2_port_factory_client_builder_h       cli_builder = NULL;
    int  ret;
    int  retries = 100; /* 100 × 100 ms = 10 s */

    iox2_set_log_level_from_env_or(iox2_log_level_e_WARN);

    nb = iox2_node_builder_new(NULL);
    ret = iox2_node_builder_create(nb, NULL, iox2_service_type_e_IPC, &tdb_iox2_node);
    if (ret != IOX2_OK)
        ereport(ERROR, (errmsg("treedb: iox2_node_builder_create failed: %d", ret)));

    ret = iox2_service_name_new(NULL, TDB_SERVICE_NAME,
                                strlen(TDB_SERVICE_NAME), &svc_name);
    if (ret != IOX2_OK)
        ereport(ERROR, (errmsg("treedb: iox2_service_name_new failed: %d", ret)));

    svc_builder = iox2_node_service_builder(&tdb_iox2_node, NULL,
                                            iox2_cast_service_name_ptr(svc_name));
    sb_rr = iox2_service_builder_request_response(svc_builder);

    /* Must match the type details the background-worker server uses. */
    iox2_service_builder_request_response_set_request_payload_type_details(
            &sb_rr, iox2_type_variant_e_DYNAMIC, "u8", 2, 1, 1);
    iox2_service_builder_request_response_set_response_payload_type_details(
            &sb_rr, iox2_type_variant_e_DYNAMIC, "u8", 2, 1, 1);

    /* Retry until the background worker has created the service. */
    while (retries-- > 0)
    {
        ret = iox2_service_builder_request_response_open_or_create(sb_rr, NULL, &service);
        if (ret == IOX2_OK)
            break;
        pg_usleep(100000L); /* 100 ms */
    }
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: cannot connect to background worker service \"%s\"",
                        TDB_SERVICE_NAME)));

    cli_builder = iox2_port_factory_request_response_client_builder(&service, NULL);
    iox2_port_factory_client_builder_set_initial_max_slice_len(
            &cli_builder, (c_size_t) TDB_CLIENT_MAX_SLICE);
    iox2_port_factory_client_builder_set_allocation_strategy(
            &cli_builder, iox2_allocation_strategy_e_POWER_OF_TWO);

    ret = iox2_port_factory_client_builder_create(cli_builder, NULL, &tdb_iox2_client);
    if (ret != IOX2_OK)
        ereport(ERROR, (errmsg("treedb: client create failed: %d", ret)));

    iox2_port_factory_request_response_drop(service);
    iox2_service_name_drop(svc_name);
}

static inline iox2_client_h *
tdb_get_client(void)
{
    if (tdb_iox2_client == NULL)
        tdb_iox2_connect();
    return &tdb_iox2_client;
}

/* ----------------------------------------------------------------
 * Single RPC over iceoryx2.
 *
 * Wire format (request slice):  [1 opcode][payload_len bytes payload]
 * Wire format (response slice): [1 status][resp_len bytes response]
 *
 * If resp_out != NULL, *resp_out is palloc'd; caller must pfree.
 * Raises ereport(ERROR) on transport error or TDB_STATUS_ERROR.
 * Returns TDB_STATUS_OK or TDB_STATUS_NOT_FOUND.
 * ---------------------------------------------------------------- */
static inline uint8
tdb_rpc(uint8 opcode,
        const void *req, uint32 req_len,
        void **resp_out, uint32 *resp_len_out)
{
    iox2_client_h          *client      = tdb_get_client();
    iox2_request_mut_h      request     = NULL;
    iox2_pending_response_h pending     = NULL;
    iox2_response_h         response    = NULL;
    uint8_t                *req_payload = NULL;
    c_size_t                total_req   = (c_size_t)(1 + req_len);
    const uint8_t          *resp_data   = NULL;
    c_size_t                resp_elems  = 0;
    uint8                   status;
    uint32                  rlen;
    int                     ret;

    /* Loan shared-memory slice for the request. */
    ret = iox2_client_loan_slice_uninit(client, NULL, &request, total_req);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: loan request slice failed: %d", ret)));

    iox2_request_mut_payload_mut(&request, (void **) &req_payload, NULL);
    req_payload[0] = opcode;
    if (req_len > 0)
        memcpy(req_payload + 1, req, (size_t) req_len);

    /* Send request. */
    ret = iox2_request_mut_send(request, NULL, &pending);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: send request failed: %d", ret)));

    /* Wait for response (poll with a short sleep to avoid spinning a full core). */
    while (true)
    {
        response = NULL;
        ret = iox2_pending_response_receive(&pending, NULL, &response);
        if (ret != IOX2_OK)
        {
            iox2_pending_response_drop(pending);
            ereport(ERROR,
                    (errmsg("treedb: pending_response_receive failed: %d", ret)));
        }
        if (response != NULL)
            break;
        sched_yield(); /* yield without sleeping — macOS usleep(10) sleeps ~100µs */
    }

    iox2_pending_response_drop(pending);

    /* Decode response: [1 status byte][resp bytes...] */
    iox2_response_payload(&response, (const void **) &resp_data, &resp_elems);

    status = (resp_elems >= 1) ? resp_data[0] : TDB_STATUS_ERROR;
    rlen   = (resp_elems > 1)  ? (uint32)(resp_elems - 1) : 0;

    if (status == TDB_STATUS_ERROR)
    {
        char errmsg_buf[256] = "unknown error from background worker";
        if (rlen > 0)
        {
            uint32 msglen = rlen < sizeof(errmsg_buf) - 1
                            ? rlen : (uint32)(sizeof(errmsg_buf) - 1);
            memcpy(errmsg_buf, resp_data + 1, msglen);
            errmsg_buf[msglen] = '\0';
        }
        iox2_response_drop(response);
        ereport(ERROR, (errmsg("treedb: %s", errmsg_buf)));
    }

    if (rlen > 0 && resp_out != NULL)
    {
        void *buf = palloc(rlen);
        memcpy(buf, resp_data + 1, rlen);
        *resp_out = buf;
        if (resp_len_out) *resp_len_out = rlen;
    }
    else
    {
        if (resp_out)     *resp_out     = NULL;
        if (resp_len_out) *resp_len_out = 0;
    }

    iox2_response_drop(response);
    return status;
}

/* ----------------------------------------------------------------
 * Encode/decode big-endian integers
 * ---------------------------------------------------------------- */
static inline void
tdb_put_u32(uint8 *p, uint32 v)
{
    p[0] = (v >> 24) & 0xFF;
    p[1] = (v >> 16) & 0xFF;
    p[2] = (v >>  8) & 0xFF;
    p[3] =  v        & 0xFF;
}

static inline void
tdb_put_u64(uint8 *p, uint64 v)
{
    p[0] = (v >> 56) & 0xFF; p[1] = (v >> 48) & 0xFF;
    p[2] = (v >> 40) & 0xFF; p[3] = (v >> 32) & 0xFF;
    p[4] = (v >> 24) & 0xFF; p[5] = (v >> 16) & 0xFF;
    p[6] = (v >>  8) & 0xFF; p[7] =  v        & 0xFF;
}

static inline uint32
tdb_get_u32(const uint8 *p)
{
    return ((uint32) p[0] << 24) | ((uint32) p[1] << 16) |
           ((uint32) p[2] <<  8) |  (uint32) p[3];
}

static inline uint64
tdb_get_u64(const uint8 *p)
{
    return ((uint64) p[0] << 56) | ((uint64) p[1] << 48) |
           ((uint64) p[2] << 40) | ((uint64) p[3] << 32) |
           ((uint64) p[4] << 24) | ((uint64) p[5] << 16) |
           ((uint64) p[6] <<  8) |  (uint64) p[7];
}

/* ----------------------------------------------------------------
 * ctid ↔ seq_num conversion
 * seq_num = block * MaxHeapTuplesPerPage + (offset - 1)
 * ---------------------------------------------------------------- */
#define TDB_TUPLES_PER_BLOCK  291  /* ~= MaxHeapTuplesPerPage at 8KB pages */

static inline uint64
tdb_ctid_to_seq(ItemPointer tid)
{
    return (uint64) ItemPointerGetBlockNumber(tid) * TDB_TUPLES_PER_BLOCK
         + (uint64)(ItemPointerGetOffsetNumber(tid) - 1);
}

static inline void
tdb_seq_to_ctid(uint64 seq, ItemPointer tid)
{
    BlockNumber  blk = (BlockNumber)(seq / TDB_TUPLES_PER_BLOCK);
    OffsetNumber off = (OffsetNumber)((seq % TDB_TUPLES_PER_BLOCK) + 1);
    ItemPointerSet(tid, blk, off);
}

/* ----------------------------------------------------------------
 * Convenience RPC wrapper used by both treedb_tam.c and treedb_bgworker.c
 * ---------------------------------------------------------------- */
static inline void
tdb_truncate_rpc(RelFileNumber relnum)
{
    uint8 req[4];
    tdb_put_u32(req, (uint32) relnum);
    tdb_rpc(TDB_OP_TRUNCATE, req, 4, NULL, NULL);
}

#endif /* TREEDB_PGEXT_H */
