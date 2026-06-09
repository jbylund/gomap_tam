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
#include <string.h>
#include <stdint.h>
#include <signal.h>

/* iceoryx2 C bindings */
#include "iox2/iceoryx2.h"

#include "treedb_pgext.h"

PG_MODULE_MAGIC;

/* Forward declarations */
PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void treedb_bgworker_main(Datum main_arg);

/* Path to the Go shared library, set at compile time via -DTDB_SHIM_PATH=... */
#ifndef TDB_SHIM_PATH
#error "TDB_SHIM_PATH must be defined at compile time (path to treedb_shim.so)"
#endif

/* Maximum response payload from a single Go call (8 KB for one tuple + 32 bytes overhead). */
#define TDB_MAX_RESP_PAYLOAD  (8 * 1024 + 32)
/* Maximum iceoryx2 request slice: 1 byte opcode + up to 8 KB tuple payload. */
#define TDB_MAX_REQ_SLICE     (8 * 1024 + 32)
/* Maximum iceoryx2 response slice: 1 byte status + TDB_MAX_RESP_PAYLOAD. */
#define TDB_MAX_RESP_SLICE    (TDB_MAX_RESP_PAYLOAD + 1)

/* iceoryx2 service names — must match treedb_pgext.h */
#define TDB_SERVICE_NAME       "treedb/bgworker"
#define TDB_EVENT_SERVICE_NAME "treedb/bgworker/event"

typedef int32_t (*treedb_init_fn)(const char *db_path);
typedef uint8_t (*treedb_handle_fn)(uint8_t opcode,
                                    const uint8_t *req, uint32_t req_len,
                                    uint8_t *resp_buf, uint32_t resp_buf_size,
                                    uint32_t *resp_len_out);

static volatile sig_atomic_t tdb_got_sigterm = 0;

static void
tdb_sigterm_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    tdb_got_sigterm = 1;
    errno = save_errno;
}

/* Context threaded through the WaitSet callback. */
typedef struct
{
    iox2_server_h    *server;
    treedb_handle_fn  handle_fn;
    uint8_t          *resp_buf;
    uint32_t          resp_buf_size;
} tdb_bgworker_ctx_t;

/*
 * tdb_process_requests_cb — WaitSet callback.
 *
 * Called by iox2_waitset_wait_and_process_once_with_timeout whenever the
 * listener fires (i.e. a backend sent a notify after writing its request).
 * Drains all pending requests in a tight loop, dispatches each to the Go
 * shim, and sends the response.  Returns CONTINUE so the WaitSet keeps
 * running; the outer while-loop is responsible for checking SIGTERM.
 */
static iox2_callback_progression_e
tdb_process_requests_cb(iox2_waitset_attachment_id_h attachment_id,
                        iox2_callback_context ctx)
{
    tdb_bgworker_ctx_t   *wctx;
    iox2_active_request_h active_req;
    iox2_response_mut_h   response;
    const uint8_t        *req_data;
    c_size_t              req_elems;
    uint8_t              *resp_payload;
    uint8_t               opcode;
    const uint8_t        *payload;
    uint32_t              payload_len;
    uint8_t               status;
    uint32_t              resp_len;
    c_size_t              total_resp;
    int                   ret;

    (void) attachment_id; /* single attachment — no need to check which */

    wctx = (tdb_bgworker_ctx_t *) ctx;

    while (true)
    {
        active_req = NULL;
        ret = iox2_server_receive(wctx->server, NULL, &active_req);
        if (ret != IOX2_OK || active_req == NULL)
            break;

        req_data  = NULL;
        req_elems = 0;
        iox2_active_request_payload(&active_req,
                                    (const void **) &req_data, &req_elems);

        if (req_elems < 1)
        {
            iox2_active_request_drop(active_req);
            continue;
        }

        opcode      = req_data[0];
        payload     = req_data + 1;
        payload_len = (uint32_t)(req_elems - 1);

        resp_len = 0;
        status = wctx->handle_fn(opcode, payload, payload_len,
                                  wctx->resp_buf, wctx->resp_buf_size, &resp_len);

        total_resp   = (c_size_t)(1 + resp_len);
        response     = NULL;
        resp_payload = NULL;
        ret = iox2_active_request_loan_slice_uninit(
                &active_req, NULL, &response, total_resp);
        if (ret != IOX2_OK)
        {
            ereport(WARNING,
                    (errmsg("treedb: loan response slice failed: %d", ret)));
            iox2_active_request_drop(active_req);
            continue;
        }

        iox2_response_mut_payload_mut(&response, (void **) &resp_payload, NULL);
        resp_payload[0] = status;
        if (resp_len > 0)
            memcpy(resp_payload + 1, wctx->resp_buf, resp_len);

        iox2_response_mut_send(response);
        iox2_active_request_drop(active_req);
    }

    return iox2_callback_progression_e_CONTINUE;
}

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
 *
 * Architecture:
 *   1. dlopen Go shim, call treedb_init() to open/create the DB directory.
 *   2. Create an iceoryx2 node + request/response service as the SERVER.
 *   3. Loop: receive requests from backends via iceoryx2, dispatch to
 *      treedb_handle(), send response back.
 */
void
treedb_bgworker_main(Datum main_arg)
{
    void               *shim_handle;
    treedb_init_fn      init_fn;
    treedb_handle_fn    handle_fn;
    char                db_path[MAXPGPATH];

    /* iceoryx2 request/response handles */
    iox2_node_builder_h                      node_builder = NULL;
    iox2_node_h                              node_handle  = NULL;
    iox2_service_name_h                      svc_name     = NULL;
    iox2_service_builder_h                   svc_builder  = NULL;
    iox2_service_builder_request_response_h  sb_rr;
    iox2_port_factory_request_response_h     service      = NULL;
    iox2_port_factory_server_builder_h       srv_builder  = NULL;
    iox2_server_h                            server       = NULL;

    /* iceoryx2 event service handles (notifier + WaitSet wake-up) */
    iox2_service_name_h                      evt_svc_name = NULL;
    iox2_service_builder_h                   evt_svc_bldr = NULL;
    iox2_service_builder_event_h             evt_sb;
    iox2_port_factory_event_h                evt_factory  = NULL;
    iox2_port_factory_listener_builder_h     lst_builder  = NULL;
    iox2_listener_h                          listener     = NULL;
    iox2_waitset_builder_h                   ws_builder   = NULL;
    iox2_waitset_h                           waitset      = NULL;
    iox2_waitset_guard_h                     guard        = NULL;
    iox2_file_descriptor_ptr                 listener_fd;

    tdb_bgworker_ctx_t       wctx;
    iox2_waitset_run_result_e ws_result;

    /* Response scratch buffer (allocated on stack — 8 KB + overhead). */
    uint8_t  resp_buf[TDB_MAX_RESP_PAYLOAD];

    int ret;

    pqsignal(SIGTERM, tdb_sigterm_handler);
    BackgroundWorkerUnblockSignals();

    tdb_db_path(db_path, sizeof(db_path));

    if (mkdir(db_path, 0700) < 0 && errno != EEXIST)
        ereport(ERROR,
                (errmsg("treedb: could not create data directory \"%s\": %m",
                        db_path)));

    /* --- Load Go shim --- */
    shim_handle = dlopen(TDB_SHIM_PATH, RTLD_NOW | RTLD_LOCAL);
    if (!shim_handle)
        ereport(ERROR,
                (errmsg("treedb: dlopen(\"%s\") failed: %s",
                        TDB_SHIM_PATH, dlerror())));

    init_fn = (treedb_init_fn) dlsym(shim_handle, "treedb_init");
    if (!init_fn)
        ereport(ERROR,
                (errmsg("treedb: dlsym(treedb_init) failed: %s", dlerror())));

    handle_fn = (treedb_handle_fn) dlsym(shim_handle, "treedb_handle");
    if (!handle_fn)
        ereport(ERROR,
                (errmsg("treedb: dlsym(treedb_handle) failed: %s", dlerror())));

    if (init_fn(db_path) != 0)
        ereport(ERROR,
                (errmsg("treedb: treedb_init(\"%s\") failed", db_path)));

    /* --- Set up iceoryx2 node --- */
    iox2_set_log_level_from_env_or(iox2_log_level_e_WARN);

    node_builder = iox2_node_builder_new(NULL);
    ret = iox2_node_builder_create(node_builder, NULL,
                                   iox2_service_type_e_IPC, &node_handle);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: iox2_node_builder_create failed: %d", ret)));

    /* --- Set up event service (listener + WaitSet) ---
     *
     * Created BEFORE the request/response service so that by the time a
     * client successfully opens the rr service, the event service already
     * exists and the client can open it without retrying.
     */
    ret = iox2_service_name_new(NULL, TDB_EVENT_SERVICE_NAME,
                                strlen(TDB_EVENT_SERVICE_NAME), &evt_svc_name);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: event service_name_new failed: %d", ret)));

    evt_svc_bldr = iox2_node_service_builder(&node_handle, NULL,
                                             iox2_cast_service_name_ptr(evt_svc_name));
    evt_sb = iox2_service_builder_event(evt_svc_bldr);
    /* Allow up to 64 concurrent backend notifiers; only 1 listener (us). */
    iox2_service_builder_event_set_max_notifiers(&evt_sb, 64);
    iox2_service_builder_event_set_max_listeners(&evt_sb, 1);
    ret = iox2_service_builder_event_open_or_create(evt_sb, NULL, &evt_factory);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: event service open_or_create failed: %d", ret)));

    lst_builder = iox2_port_factory_event_listener_builder(&evt_factory, NULL);
    ret = iox2_port_factory_listener_builder_create(lst_builder, NULL, &listener);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: listener create failed: %d", ret)));

    listener_fd = iox2_listener_get_file_descriptor(&listener);

    iox2_waitset_builder_new(NULL, &ws_builder);
    /* Disable iceoryx2 signal handling — we manage SIGTERM ourselves. */
    iox2_waitset_builder_set_signal_handling_mode(&ws_builder,
                                                  iox2_signal_handling_mode_e_DISABLED);
    ret = iox2_waitset_builder_create(ws_builder, iox2_service_type_e_IPC,
                                      NULL, &waitset);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: waitset create failed: %d", ret)));

    ret = iox2_waitset_attach_notification(&waitset, listener_fd, NULL, &guard);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: waitset attach_notification failed: %d", ret)));

    iox2_port_factory_event_drop(evt_factory);
    iox2_service_name_drop(evt_svc_name);

    /* --- Set up request/response service --- */
    ret = iox2_service_name_new(NULL, TDB_SERVICE_NAME,
                                strlen(TDB_SERVICE_NAME), &svc_name);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: iox2_service_name_new failed: %d", ret)));

    svc_builder = iox2_node_service_builder(&node_handle, NULL,
                                            iox2_cast_service_name_ptr(svc_name));
    sb_rr = iox2_service_builder_request_response(svc_builder);

    ret = iox2_service_builder_request_response_set_request_payload_type_details(
            &sb_rr, iox2_type_variant_e_DYNAMIC, "u8", 2, 1, 1);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: set request type details failed: %d", ret)));

    ret = iox2_service_builder_request_response_set_response_payload_type_details(
            &sb_rr, iox2_type_variant_e_DYNAMIC, "u8", 2, 1, 1);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: set response type details failed: %d", ret)));

    ret = iox2_service_builder_request_response_open_or_create(sb_rr, NULL, &service);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: open_or_create service failed: %d", ret)));

    srv_builder = iox2_port_factory_request_response_server_builder(&service, NULL);
    iox2_port_factory_server_builder_set_initial_max_slice_len(
            &srv_builder, (c_size_t) TDB_MAX_RESP_SLICE);

    ret = iox2_port_factory_server_builder_create(srv_builder, NULL, &server);
    if (ret != IOX2_OK)
        ereport(ERROR,
                (errmsg("treedb: server create failed: %d", ret)));

    iox2_service_name_drop(svc_name);
    iox2_port_factory_request_response_drop(service);

    ereport(LOG,
            (errmsg("treedb background worker ready: db=%s service=%s",
                    db_path, TDB_SERVICE_NAME)));

    /* --- Main event loop ---
     *
     * iox2_waitset_wait_and_process_once_with_timeout blocks on the listener's
     * file descriptor (kqueue on macOS, epoll on Linux) until a backend fires
     * iox2_notifier_notify after writing its request.  The callback drains all
     * pending requests and sends responses.  The 1-second timeout ensures we
     * wake up to check SIGTERM even if no requests arrive.
     */
    wctx.server        = &server;
    wctx.handle_fn     = handle_fn;
    wctx.resp_buf      = resp_buf;
    wctx.resp_buf_size = sizeof(resp_buf);

    while (!tdb_got_sigterm)
    {
        ws_result = (iox2_waitset_run_result_e) 0;
        ret = iox2_waitset_wait_and_process_once_with_timeout(
                &waitset, tdb_process_requests_cb, &wctx,
                1 /* seconds */, 0 /* nanoseconds */, &ws_result);
        if (ret != IOX2_OK)
            ereport(WARNING,
                    (errmsg("treedb: waitset error %d (result %d); continuing",
                            ret, (int) ws_result)));
    }

    /* Cleanup */
    iox2_waitset_guard_drop(guard);
    iox2_waitset_drop(waitset);
    iox2_listener_drop(listener);
    iox2_server_drop(server);
    iox2_node_drop(node_handle);
    dlclose(shim_handle);
    proc_exit(0);
}
