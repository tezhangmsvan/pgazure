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


/*
 * blob_storage_get_blob
 */
Datum
blob_storage_get_blob(PG_FUNCTION_ARGS)
{
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
