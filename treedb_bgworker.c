#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/memutils.h"
#include "utils/guc.h"

#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "nodes/pg_list.h"
#include "utils/syscache.h"

#include <dlfcn.h>
#include <sys/stat.h>
#include <errno.h>

#include "treedb_pgext.h"

PG_MODULE_MAGIC;

/* Forward declarations */
PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void treedb_bgworker_main(Datum main_arg);

/* Path to the Go shared library, set at compile time via -DTDB_SHIM_PATH=... */
#ifndef TDB_SHIM_PATH
#error "TDB_SHIM_PATH must be defined at compile time (path to treedb_shim.so)"
#endif

typedef void (*treedb_serve_fn)(const char *socket_path, const char *db_path);

/* ----------------------------------------------------------------
 * DROP TABLE cleanup
 *
 * PostgreSQL has no TAM callback for DROP TABLE, so we hook into
 * object_access_hook to detect when a treedb table is dropped and
 * record its relfilenode.  On transaction commit we send a TRUNCATE
 * RPC to delete all TreeDB data for that relfilenode; on abort we
 * discard the list (the table wasn't actually dropped).
 * ---------------------------------------------------------------- */

static object_access_hook_type prev_object_access_hook = NULL;
static List *treedb_pending_drops = NIL;

static void
treedb_xact_callback(XactEvent event, void *arg)
{
    if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_PARALLEL_COMMIT)
    {
        ListCell *lc;
        foreach(lc, treedb_pending_drops)
            tdb_truncate_rpc((RelFileNumber) lfirst_int(lc));
    }

    if (event == XACT_EVENT_COMMIT   || event == XACT_EVENT_ABORT ||
        event == XACT_EVENT_PARALLEL_COMMIT || event == XACT_EVENT_PARALLEL_ABORT)
    {
        list_free(treedb_pending_drops);
        treedb_pending_drops = NIL;
    }
}

static void
treedb_object_access(ObjectAccessType access, Oid classId, Oid objectId,
                     int subId, void *arg)
{
    HeapTuple       tuple;
    Form_pg_class   relform;
    Oid             treedb_am;
    RelFileNumber   relnum;

    /* Chain to any previously registered hook. */
    if (prev_object_access_hook)
        prev_object_access_hook(access, classId, objectId, subId, arg);

    /* Only care about DROP on pg_class entries (i.e. relations). */
    if (access != OAT_DROP || classId != RelationRelationId)
        return;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(objectId));
    if (!HeapTupleIsValid(tuple))
        return;

    relform = (Form_pg_class) GETSTRUCT(tuple);

    if (relform->relkind != RELKIND_RELATION)
    {
        ReleaseSysCache(tuple);
        return;
    }

    treedb_am = get_table_am_oid("treedb", true);
    if (!OidIsValid(treedb_am) || relform->relam != treedb_am)
    {
        ReleaseSysCache(tuple);
        return;
    }

    relnum = relform->relfilenode;
    ReleaseSysCache(tuple);

    /* Defer the actual cleanup until commit so aborted DROPs don't wipe data. */
    {
        MemoryContext oldcxt = MemoryContextSwitchTo(TopTransactionContext);
        treedb_pending_drops = lappend_int(treedb_pending_drops, (int) relnum);
        MemoryContextSwitchTo(oldcxt);
    }
}

/*
 * _PG_init: called when the extension is loaded.
 * Registers the background worker and DROP TABLE cleanup hooks.
 */
void
_PG_init(void)
{
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        ereport(ERROR,
                (errmsg("treedb_pgext must be loaded via shared_preload_libraries")));

    /* Register DROP TABLE cleanup hooks (fire in backend processes). */
    prev_object_access_hook = object_access_hook;
    object_access_hook      = treedb_object_access;
    RegisterXactCallback(treedb_xact_callback, NULL);

    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time   = BgWorkerStart_PostmasterStart;
    worker.bgw_restart_time = 5; /* restart after 5s on crash */
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "treedb_pgext");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "treedb_bgworker_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "treedb background worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "treedb");
    worker.bgw_main_arg     = (Datum) 0;
    worker.bgw_notify_pid   = 0;

    RegisterBackgroundWorker(&worker);
}

/*
 * treedb_bgworker_main: entry point for the background worker process.
 */
void
treedb_bgworker_main(Datum main_arg)
{
    void            *shim_handle;
    treedb_serve_fn  serve_fn;
    char             socket_path[MAXPGPATH];
    char             db_path[MAXPGPATH];

    BackgroundWorkerUnblockSignals();

    tdb_socket_path(socket_path, sizeof(socket_path));
    tdb_db_path(db_path, sizeof(db_path));

    if (mkdir(db_path, 0700) < 0 && errno != EEXIST)
        ereport(ERROR,
                (errmsg("treedb: could not create data directory \"%s\": %m",
                        db_path)));

    shim_handle = dlopen(TDB_SHIM_PATH, RTLD_NOW | RTLD_LOCAL);
    if (!shim_handle)
        ereport(ERROR,
                (errmsg("treedb: dlopen(\"%s\") failed: %s",
                        TDB_SHIM_PATH, dlerror())));

    serve_fn = (treedb_serve_fn) dlsym(shim_handle, "treedb_serve");
    if (!serve_fn)
        ereport(ERROR,
                (errmsg("treedb: dlsym(treedb_serve) failed: %s", dlerror())));

    ereport(LOG,
            (errmsg("treedb background worker starting: db=%s socket=%s",
                    db_path, socket_path)));

    serve_fn(socket_path, db_path);

    dlclose(shim_handle);
    proc_exit(0);
}
