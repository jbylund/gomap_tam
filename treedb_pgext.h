#ifndef TREEDB_PGEXT_H
#define TREEDB_PGEXT_H

#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "postgres.h"
#include "miscadmin.h"   /* DataDir */
#include "storage/pg_shmem.h"

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

/* Maximum payload we'll ever send in a single RPC (8 KB for one tuple). */
#define TDB_MAX_TUPLE_BYTES  (8 * 1024)

/* ----------------------------------------------------------------
 * Socket path helpers
 * ---------------------------------------------------------------- */
static inline void
tdb_socket_path(char *buf, size_t bufsz)
{
    snprintf(buf, bufsz, "%s/treedb.sock", DataDir);
}

static inline void
tdb_db_path(char *buf, size_t bufsz)
{
    snprintf(buf, bufsz, "%s/treedb_data", DataDir);
}

/* ----------------------------------------------------------------
 * Persistent per-process connection to the background worker.
 *
 * Each backend holds a single open Unix socket that is reused across
 * all RPCs.  tdb_conn_reset() closes and invalidates it; the next
 * tdb_rpc() call will reconnect automatically.  The OS closes the fd
 * when the backend process exits.
 * ---------------------------------------------------------------- */
static int tdb_conn_fd = -1;

static inline void
tdb_conn_reset(void)
{
    if (tdb_conn_fd >= 0)
    {
        close(tdb_conn_fd);
        tdb_conn_fd = -1;
    }
}

/* ----------------------------------------------------------------
 * Low-level I/O helpers
 * ---------------------------------------------------------------- */
static inline void
tdb_write_all(int fd, const void *buf, int len)
{
    const char *ptr = (const char *) buf;
    while (len > 0)
    {
        int n = (int) send(fd, ptr, len, 0);
        if (n <= 0)
        {
            tdb_conn_reset(); /* mark broken; next RPC will reconnect */
            ereport(ERROR, (errmsg("treedb: send failed: %m")));
        }
        ptr += n;
        len -= n;
    }
}

static inline void
tdb_read_all(int fd, void *buf, int len)
{
    char *ptr = (char *) buf;
    while (len > 0)
    {
        int n = (int) recv(fd, ptr, len, 0);
        if (n <= 0)
        {
            tdb_conn_reset(); /* mark broken; next RPC will reconnect */
            ereport(ERROR, (errmsg("treedb: recv failed: %m")));
        }
        ptr += n;
        len -= n;
    }
}

/* Open a fresh connection with retry (up to 10 s for bgworker startup). */
static inline int
tdb_connect(void)
{
    char        sockpath[MAXPGPATH];
    struct sockaddr_un addr;
    int         fd;
    int         retries = 100; /* 100 × 100 ms = 10 s */

    tdb_socket_path(sockpath, sizeof(sockpath));
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strlcpy(addr.sun_path, sockpath, sizeof(addr.sun_path));

    while (retries-- > 0)
    {
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0)
            ereport(ERROR, (errmsg("treedb: socket(): %m")));

        if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) == 0)
            return fd;

        close(fd);
        pg_usleep(100000L); /* 100 ms */
    }

    ereport(ERROR,
            (errmsg("treedb: cannot connect to background worker at %s",
                    sockpath)));
    return -1; /* not reached */
}

/* Return the persistent fd, connecting lazily on first call. */
static inline int
tdb_get_connection(void)
{
    if (tdb_conn_fd < 0)
        tdb_conn_fd = tdb_connect();
    return tdb_conn_fd;
}

/* ----------------------------------------------------------------
 * Single RPC over the persistent connection.
 * If resp_out != NULL, *resp_out is palloc'd; caller must pfree.
 * Raises ereport(ERROR) on I/O error or TDB_STATUS_ERROR.
 * Returns TDB_STATUS_OK or TDB_STATUS_NOT_FOUND.
 * ---------------------------------------------------------------- */
static inline uint8
tdb_rpc(uint8 opcode,
        const void *req, uint32 req_len,
        void **resp_out, uint32 *resp_len_out)
{
    int     fd = tdb_get_connection();
    uint8   hdr[5];
    uint8   resp_hdr[5];
    uint8   status;
    uint32  rlen;

    /* Write request header + payload */
    hdr[0] = opcode;
    hdr[1] = (req_len >> 24) & 0xFF;
    hdr[2] = (req_len >> 16) & 0xFF;
    hdr[3] = (req_len >> 8)  & 0xFF;
    hdr[4] =  req_len        & 0xFF;
    tdb_write_all(fd, hdr, 5);
    if (req_len > 0)
        tdb_write_all(fd, req, (int) req_len);

    /* Read response header */
    tdb_read_all(fd, resp_hdr, 5);
    status = resp_hdr[0];
    rlen   = ((uint32) resp_hdr[1] << 24) |
             ((uint32) resp_hdr[2] << 16) |
             ((uint32) resp_hdr[3] <<  8) |
              (uint32) resp_hdr[4];

    /* Read response payload */
    if (rlen > 0)
    {
        void *buf = palloc(rlen);
        tdb_read_all(fd, buf, (int) rlen);
        if (resp_out)      *resp_out     = buf;
        if (resp_len_out)  *resp_len_out = rlen;

        if (status == TDB_STATUS_ERROR)
        {
            char errmsg_buf[256];
            uint32 msglen = rlen < sizeof(errmsg_buf) - 1 ? rlen : sizeof(errmsg_buf) - 1;
            memcpy(errmsg_buf, buf, msglen);
            errmsg_buf[msglen] = '\0';
            ereport(ERROR, (errmsg("treedb: %s", errmsg_buf)));
        }
    }
    else
    {
        if (resp_out)     *resp_out     = NULL;
        if (resp_len_out) *resp_len_out = 0;

        if (status == TDB_STATUS_ERROR)
            ereport(ERROR, (errmsg("treedb: unknown error from background worker")));
    }

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
 * Convenience RPC wrappers used by both treedb_tam.c and treedb_bgworker.c
 * ---------------------------------------------------------------- */
static inline void
tdb_truncate_rpc(RelFileNumber relnum)
{
    uint8 req[4];
    tdb_put_u32(req, (uint32) relnum);
    tdb_rpc(TDB_OP_TRUNCATE, req, 4, NULL, NULL);
}

#endif /* TREEDB_PGEXT_H */
