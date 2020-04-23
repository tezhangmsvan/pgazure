CREATE FUNCTION blob_storage_list_blobs(connection_string text, container_name text, prefix text, OUT path text, OUT size bigint, OUT last_modified timestamptz, OUT etag text, OUT content_type text, OUT content_encoding text, OUT content_language text, OUT content_md5 text)
    RETURNS SETOF record
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$blob_storage_list_blobs$$;
COMMENT ON FUNCTION blob_storage_list_blobs(text,text,text)
    IS 'list blobs in blob storage';

CREATE FUNCTION blob_storage_get_blob(connection_string text, container_name text, path text, decoder text default 'auto', compression text default 'auto')
    RETURNS SETOF record
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$blob_storage_get_blob$$;
COMMENT ON FUNCTION blob_storage_get_blob(text,text,text,text,text)
    IS 'get a blob from blob storage';

CREATE FUNCTION blob_storage_get_blob(connection_string text, container_name text, path text, rec anyelement, decoder text default 'auto', compression text default 'auto')
    RETURNS SETOF anyelement
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$blob_storage_get_blob_anyelement$$;
COMMENT ON FUNCTION blob_storage_get_blob(text,text,text,anyelement,text,text)
    IS 'get a blob from blob storage';

CREATE FUNCTION blob_storage_put_blob_sfunc(state internal, connection_string text, container_name text, path text, tuple record)
RETURNS internal
LANGUAGE C
CALLED ON NULL INPUT
AS 'MODULE_PATHNAME', $$blob_storage_put_blob_sfunc$$;

CREATE FUNCTION blob_storage_put_blob_sfunc(state internal, connection_string text, container_name text, path text, tuple record, encoder text)
RETURNS internal
LANGUAGE C
CALLED ON NULL INPUT
AS 'MODULE_PATHNAME', $$blob_storage_put_blob_sfunc$$;

CREATE FUNCTION blob_storage_put_blob_sfunc(state internal, connection_string text, container_name text, path text, tuple record, encoder text, compression text)
RETURNS internal
LANGUAGE C
CALLED ON NULL INPUT
AS 'MODULE_PATHNAME', $$blob_storage_put_blob_sfunc$$;

CREATE FUNCTION blob_storage_put_blob_final(state internal)
RETURNS VOID
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$blob_storage_put_blob_final$$;

CREATE AGGREGATE blob_storage_put_blob(connection_string text, container_name text, path text, tuple record) (
    stype=internal,
    sfunc=blob_storage_put_blob_sfunc,
    finalfunc=blob_storage_put_blob_final
);

CREATE AGGREGATE blob_storage_put_blob(connection_string text, container_name text, path text, tuple record, encoder text) (
    stype=internal,
    sfunc=blob_storage_put_blob_sfunc,
    finalfunc=blob_storage_put_blob_final
);

CREATE AGGREGATE blob_storage_put_blob(connection_string text, container_name text, path text, tuple record, encoder text, compression text) (
    stype=internal,
    sfunc=blob_storage_put_blob_sfunc,
    finalfunc=blob_storage_put_blob_final
);
