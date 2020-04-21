/*-------------------------------------------------------------------------
 *
 * src/get_blob.c
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <zlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "nodes/makefuncs.h"
#include "pgazure/blob_storage.h"
#include "pgazure/copy_format_decoder.h"
#include "pgazure/set_returning_functions.h"
#include "utils/builtins.h"


PG_FUNCTION_INFO_V1(blob_storage_get_blob);
PG_FUNCTION_INFO_V1(blob_storage_get_blob_anyelement);


/*
 * blob_storage_get_blob
 */
Datum
blob_storage_get_blob(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("connection_string is required")));
	}
	if (PG_ARGISNULL(1))
	{
		ereport(ERROR, (errmsg("container_name is required")));
	}
	if (PG_ARGISNULL(2))
	{
		ereport(ERROR, (errmsg("path is required")));
	}

	char *connectionString = text_to_cstring(PG_GETARG_TEXT_P(0));
	char *containerName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char *path = text_to_cstring(PG_GETARG_TEXT_P(2));
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	ByteSource byteSource;

	ReadBlockBlob(connectionString, containerName, path, &byteSource);

	DefElem *formatResultOption = makeDefElem("format", (Node *) makeString("csv"), -1);
	List *copyOptions = list_make1(formatResultOption);

	DecodeCopyFormatByteSourceIntoTupleStore(&byteSource,
                                             tupleStore,
                                             tupleDescriptor,
                                             copyOptions);

	PG_RETURN_DATUM(0);
}


/*
 * blob_storage_get_blob
 */
Datum
blob_storage_get_blob_anyelement(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("connection_string is required")));
	}
	if (PG_ARGISNULL(1))
	{
		ereport(ERROR, (errmsg("container_name is required")));
	}
	if (PG_ARGISNULL(2))
	{
		ereport(ERROR, (errmsg("path is required")));
	}

	char *connectionString = text_to_cstring(PG_GETARG_TEXT_P(0));
	char *containerName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char *path = text_to_cstring(PG_GETARG_TEXT_P(2));

	Oid typeId = get_fn_expr_argtype(fcinfo->flinfo, 3);
	TupleDesc tupleDescriptor = TypeGetTupleDesc(typeId, NIL);
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	ByteSource byteSource;

	ReadBlockBlob(connectionString, containerName, path, &byteSource);

	DefElem *formatResultOption = makeDefElem("format", (Node *) makeString("csv"), -1);
	List *copyOptions = list_make1(formatResultOption);

	DecodeCopyFormatByteSourceIntoTupleStore(&byteSource,
                                             tupleStore,
                                             tupleDescriptor,
                                             copyOptions);

	PG_RETURN_DATUM(0);
}
