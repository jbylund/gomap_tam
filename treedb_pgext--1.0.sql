\echo Use "CREATE EXTENSION treedb_pgext" to load this file. \quit

CREATE FUNCTION treedb_am_handler(internal)
    RETURNS table_am_handler
    AS '$libdir/treedb_pgext'
    LANGUAGE C;

CREATE ACCESS METHOD treedb
    TYPE TABLE
    HANDLER treedb_am_handler;
