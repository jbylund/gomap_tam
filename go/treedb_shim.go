package main

/*
#include <stdint.h>
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Protocol opcodes (must match treedb_pgext.h)
const (
	opInsert      = 0x01
	opScanBegin   = 0x02
	opScanNext    = 0x03
	opScanEnd     = 0x04
	opFetch       = 0x05
	opDelete      = 0x06
	opTruncate    = 0x07
	opCount       = 0x08
	opUpdate      = 0x09
	opInsertKeyed = 0x0A
	opRekey       = 0x0B
)

const (
	statusOK       = 0x00
	statusNotFound = 0x01
	statusError    = 0x02
)

// Iterator bounds covering all valid seq_num values (uint64 big-endian).
var (
	dbKeyMin = make([]byte, 8)
	dbKeyMax = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

// Per-scan state kept in the background worker.
type scanState struct {
	mu   sync.Mutex
	iter treedb.Iterator
}

var (
	baseDBPath string

	dbMap   = map[uint32]*treedb.DB{}
	dbMapMu sync.Mutex

	scans      = map[uint64]*scanState{}
	scansMu    sync.Mutex
	nextScanID atomic.Uint64

	seqNums   sync.Map // relfilenode(uint32) -> *atomic.Uint64
	rowCounts sync.Map // relfilenode(uint32) -> *atomic.Int64
)

// seqKey builds the 8-byte TreeDB key for a seq_num.
func seqKey(seqNum uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, seqNum)
	return k
}

// getDB returns the open TreeDB for a relation, opening it lazily on first access.
func getDB(relfilenode uint32) (*treedb.DB, error) {
	dbMapMu.Lock()
	defer dbMapMu.Unlock()
	if db, ok := dbMap[relfilenode]; ok {
		return db, nil
	}
	dir := filepath.Join(baseDBPath, fmt.Sprintf("%d", relfilenode))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		return nil, err
	}
	dbMap[relfilenode] = db
	return db, nil
}

// findMaxSeqNum scans backwards to find the highest seq_num in a relation's DB.
func findMaxSeqNum(db *treedb.DB) uint64 {
	it, err := db.ReverseIterator(dbKeyMin, dbKeyMax)
	if err != nil || it == nil {
		return 0
	}
	defer it.Close()
	if !it.Valid() {
		return 0
	}
	k := it.Key()
	if len(k) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(k[0:8])
}

// countRows scans to count live keys (used for lazy row-count init only).
func countRows(db *treedb.DB) int64 {
	iter, err := db.Iterator(dbKeyMin, dbKeyMax)
	if err != nil || iter == nil {
		return 0
	}
	defer iter.Close()
	var n int64
	for iter.Valid() {
		n++
		iter.Next()
	}
	return n
}

// getRowCount returns the live row counter for a relation, initializing lazily via scan.
func getRowCount(db *treedb.DB, relfilenode uint32) *atomic.Int64 {
	if v, ok := rowCounts.Load(relfilenode); ok {
		return v.(*atomic.Int64)
	}
	c := &atomic.Int64{}
	c.Store(countRows(db))
	actual, _ := rowCounts.LoadOrStore(relfilenode, c)
	return actual.(*atomic.Int64)
}

// nextSeqNum returns the next available seq_num for a relation (thread-safe).
func nextSeqNum(db *treedb.DB, relfilenode uint32) uint64 {
	if v, ok := seqNums.Load(relfilenode); ok {
		return v.(*atomic.Uint64).Add(1) - 1
	}
	c := &atomic.Uint64{}
	c.Store(findMaxSeqNum(db) + 1)
	actual, _ := seqNums.LoadOrStore(relfilenode, c)
	return actual.(*atomic.Uint64).Add(1) - 1
}

// ---- Request handlers ----

func handleInsert(payload []byte) (byte, []byte) {
	if len(payload) < 8 {
		return statusError, []byte("insert: payload too short")
	}
	relfilenode := binary.BigEndian.Uint32(payload[0:4])
	tupleLen := binary.BigEndian.Uint32(payload[4:8])
	if uint32(len(payload)) < 8+tupleLen {
		return statusError, []byte("insert: truncated tuple")
	}
	tupleData := payload[8 : 8+tupleLen]

	db, err := getDB(relfilenode)
	if err != nil {
		return statusError, []byte(err.Error())
	}
	seqNum := nextSeqNum(db, relfilenode)
	if err := db.Set(seqKey(seqNum), tupleData); err != nil {
		return statusError, []byte(err.Error())
	}
	getRowCount(db, relfilenode).Add(1)

	resp := make([]byte, 8)
	binary.BigEndian.PutUint64(resp, seqNum)
	return statusOK, resp
}

func handleScanBegin(payload []byte) (byte, []byte) {
	if len(payload) < 4 {
		return statusError, []byte("scan_begin: payload too short")
	}
	relfilenode := binary.BigEndian.Uint32(payload[0:4])

	db, err := getDB(relfilenode)
	if err != nil {
		return statusError, []byte(err.Error())
	}
	iter, err := db.Iterator(dbKeyMin, dbKeyMax)
	if err != nil {
		return statusError, []byte(err.Error())
	}

	scanID := nextScanID.Add(1)
	scansMu.Lock()
	scans[scanID] = &scanState{iter: iter}
	scansMu.Unlock()

	resp := make([]byte, 8)
	binary.BigEndian.PutUint64(resp, scanID)
	return statusOK, resp
}

func handleScanNext(payload []byte) (byte, []byte) {
	if len(payload) < 8 {
		return statusError, []byte("scan_next: payload too short")
	}
	scanID := binary.BigEndian.Uint64(payload[0:8])

	scansMu.Lock()
	state, ok := scans[scanID]
	scansMu.Unlock()
	if !ok {
		return statusError, []byte("scan_next: unknown scan id")
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	for {
		if !state.iter.Valid() {
			if err := state.iter.Error(); err != nil {
				return statusError, []byte(err.Error())
			}
			return statusNotFound, nil
		}

		key := state.iter.KeyCopy(nil)
		val := state.iter.ValueCopy(nil)
		state.iter.Next()

		if len(key) < 8 {
			return statusError, []byte("scan_next: malformed key")
		}
		seqNum := binary.BigEndian.Uint64(key[0:8])

		if len(val) == 0 {
			continue // skip tombstone
		}

		resp := make([]byte, 8+4+len(val))
		binary.BigEndian.PutUint64(resp[0:8], seqNum)
		binary.BigEndian.PutUint32(resp[8:12], uint32(len(val)))
		copy(resp[12:], val)
		return statusOK, resp
	}
}

func handleScanEnd(payload []byte) (byte, []byte) {
	if len(payload) < 8 {
		return statusError, []byte("scan_end: payload too short")
	}
	scanID := binary.BigEndian.Uint64(payload[0:8])

	scansMu.Lock()
	state, ok := scans[scanID]
	if ok {
		delete(scans, scanID)
	}
	scansMu.Unlock()

	if ok {
		state.mu.Lock()
		state.iter.Close()
		state.mu.Unlock()
	}
	return statusOK, nil
}

func handleFetch(payload []byte) (byte, []byte) {
	if len(payload) < 12 {
		return statusError, []byte("fetch: payload too short")
	}
	relfilenode := binary.BigEndian.Uint32(payload[0:4])
	seqNum := binary.BigEndian.Uint64(payload[4:12])

	db, err := getDB(relfilenode)
	if err != nil {
		return statusError, []byte(err.Error())
	}
	val, err := db.Get(seqKey(seqNum))
	if err != nil {
		if errors.Is(err, treedb.ErrKeyNotFound) {
			return statusNotFound, nil
		}
		return statusError, []byte(err.Error())
	}
	if len(val) == 0 {
		return statusNotFound, nil
	}
	return statusOK, val
}

func handleDelete(payload []byte) (byte, []byte) {
	if len(payload) < 12 {
		return statusError, []byte("delete: payload too short")
	}
	relfilenode := binary.BigEndian.Uint32(payload[0:4])
	seqNum := binary.BigEndian.Uint64(payload[4:12])

	db, err := getDB(relfilenode)
	if err != nil {
		return statusError, []byte(err.Error())
	}
	if err := db.Delete(seqKey(seqNum)); err != nil {
		return statusError, []byte(err.Error())
	}
	getRowCount(db, relfilenode).Add(-1)
	return statusOK, nil
}

func handleCount(payload []byte) (byte, []byte) {
	if len(payload) < 4 {
		return statusError, []byte("count: payload too short")
	}
	relfilenode := binary.BigEndian.Uint32(payload[0:4])

	db, err := getDB(relfilenode)
	if err != nil {
		return statusError, []byte(err.Error())
	}
	var count int64
	if c := getRowCount(db, relfilenode); c != nil {
		count = c.Load()
	}
	if count < 0 {
		count = 0
	}

	resp := make([]byte, 8)
	binary.BigEndian.PutUint64(resp, uint64(count))
	return statusOK, resp
}

func handleUpdate(payload []byte) (byte, []byte) {
	if len(payload) < 16 {
		return statusError, []byte("update: payload too short")
	}
	relfilenode := binary.BigEndian.Uint32(payload[0:4])
	oldSeqNum := binary.BigEndian.Uint64(payload[4:12])
	tupleLen := binary.BigEndian.Uint32(payload[12:16])
	if uint32(len(payload)) < 16+tupleLen {
		return statusError, []byte("update: truncated tuple")
	}
	tupleData := payload[16 : 16+tupleLen]

	db, err := getDB(relfilenode)
	if err != nil {
		return statusError, []byte(err.Error())
	}
	// Overwrite same key in-place — ctid stays the same, row count unchanged.
	if err := db.Set(seqKey(oldSeqNum), tupleData); err != nil {
		return statusError, []byte(err.Error())
	}

	resp := make([]byte, 8)
	binary.BigEndian.PutUint64(resp, oldSeqNum)
	return statusOK, resp
}

func handleInsertKeyed(payload []byte) (byte, []byte) {
	if len(payload) < 16 {
		return statusError, []byte("insert_keyed: payload too short")
	}
	relfilenode := binary.BigEndian.Uint32(payload[0:4])
	key := binary.BigEndian.Uint64(payload[4:12])
	tupleLen := binary.BigEndian.Uint32(payload[12:16])
	if uint32(len(payload)) < 16+tupleLen {
		return statusError, []byte("insert_keyed: truncated tuple")
	}
	tupleData := payload[16 : 16+tupleLen]

	db, err := getDB(relfilenode)
	if err != nil {
		return statusError, []byte(err.Error())
	}
	if err := db.Set(seqKey(key), tupleData); err != nil {
		return statusError, []byte(err.Error())
	}
	getRowCount(db, relfilenode).Add(1)

	resp := make([]byte, 8)
	binary.BigEndian.PutUint64(resp, key)
	return statusOK, resp
}

func handleRekey(payload []byte) (byte, []byte) {
	if len(payload) < 20 {
		return statusError, []byte("rekey: payload too short")
	}
	relfilenode := binary.BigEndian.Uint32(payload[0:4])
	oldKey := binary.BigEndian.Uint64(payload[4:12])
	newKey := binary.BigEndian.Uint64(payload[12:20])

	db, err := getDB(relfilenode)
	if err != nil {
		return statusError, []byte(err.Error())
	}
	val, err := db.Get(seqKey(oldKey))
	if err != nil {
		if errors.Is(err, treedb.ErrKeyNotFound) {
			return statusNotFound, nil
		}
		return statusError, []byte(err.Error())
	}
	if err := db.Set(seqKey(newKey), val); err != nil {
		return statusError, []byte(err.Error())
	}
	if err := db.Delete(seqKey(oldKey)); err != nil {
		return statusError, []byte(err.Error())
	}
	return statusOK, nil
}

func handleTruncate(payload []byte) (byte, []byte) {
	if len(payload) < 4 {
		return statusError, []byte("truncate: payload too short")
	}
	relfilenode := binary.BigEndian.Uint32(payload[0:4])

	seqNums.Delete(relfilenode)
	rowCounts.Delete(relfilenode)

	dbMapMu.Lock()
	db, ok := dbMap[relfilenode]
	if ok {
		delete(dbMap, relfilenode)
	}
	dbMapMu.Unlock()

	if ok {
		db.Close()
	}

	dir := filepath.Join(baseDBPath, fmt.Sprintf("%d", relfilenode))
	if err := os.RemoveAll(dir); err != nil {
		return statusError, []byte(err.Error())
	}
	return statusOK, nil
}

// handleConn serves a persistent connection, processing RPCs until the client
// closes the connection or an I/O error occurs.
func handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		// Read: [1-byte opcode][4-byte payload_len BE][payload]
		var hdr [5]byte
		if _, err := io.ReadFull(conn, hdr[:]); err != nil {
			return // EOF or error: client closed connection
		}
		opcode := hdr[0]
		payloadLen := binary.BigEndian.Uint32(hdr[1:5])

		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			if _, err := io.ReadFull(conn, payload); err != nil {
				return
			}
		}

		var respStatus byte
		var respPayload []byte

		switch opcode {
		case opInsert:
			respStatus, respPayload = handleInsert(payload)
		case opScanBegin:
			respStatus, respPayload = handleScanBegin(payload)
		case opScanNext:
			respStatus, respPayload = handleScanNext(payload)
		case opScanEnd:
			respStatus, respPayload = handleScanEnd(payload)
		case opFetch:
			respStatus, respPayload = handleFetch(payload)
		case opDelete:
			respStatus, respPayload = handleDelete(payload)
		case opTruncate:
			respStatus, respPayload = handleTruncate(payload)
		case opCount:
			respStatus, respPayload = handleCount(payload)
		case opUpdate:
			respStatus, respPayload = handleUpdate(payload)
		case opInsertKeyed:
			respStatus, respPayload = handleInsertKeyed(payload)
		case opRekey:
			respStatus, respPayload = handleRekey(payload)
		default:
			respStatus = statusError
			respPayload = []byte("unknown opcode")
		}

		// Write: [1-byte status][4-byte payload_len BE][payload]
		var respHdr [5]byte
		respHdr[0] = respStatus
		binary.BigEndian.PutUint32(respHdr[1:5], uint32(len(respPayload)))
		if _, err := conn.Write(respHdr[:]); err != nil {
			return
		}
		if len(respPayload) > 0 {
			if _, err := conn.Write(respPayload); err != nil {
				return
			}
		}
	}
}

// treedb_serve sets up the base data directory, listens on socketPath, and serves
// requests until the process exits. Per-relation TreeDB instances are opened lazily
// on first access. Called from the Postgres background worker via dlopen/dlsym.
//
//export treedb_serve
func treedb_serve(socketPathC *C.char, dbPathC *C.char) {
	socketPath := C.GoString(socketPathC)
	baseDBPath = C.GoString(dbPathC)

	os.Remove(socketPath)

	if err := os.MkdirAll(baseDBPath, 0755); err != nil {
		os.Stderr.WriteString("treedb_serve: mkdir failed: " + err.Error() + "\n")
		os.Exit(1)
	}

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		os.Stderr.WriteString("treedb_serve: listen failed: " + err.Error() + "\n")
		os.Exit(1)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			break // socket closed (process shutting down)
		}
		go handleConn(conn)
	}

	// Close all open relation DBs on shutdown.
	dbMapMu.Lock()
	for _, db := range dbMap {
		db.Close()
	}
	dbMapMu.Unlock()

	os.Remove(socketPath)
}

func main() {}
