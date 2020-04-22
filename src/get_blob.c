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
#include "pgazure/codecs.h"
#include "pgazure/copy_format_decoder.h"
#include "pgazure/set_returning_functions.h"
#include "utils/builtins.h"


static void DecodeTuplesIntoTupleStore(TupleDecoder *decoder,
                                       Tuplestorestate *tupleStore);


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
	if (PG_ARGISNULL(3))
	{
		ereport(ERROR, (errmsg("decoder is required")));
	}

	char *connectionString = text_to_cstring(PG_GETARG_TEXT_P(0));
	char *containerName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char *path = text_to_cstring(PG_GETARG_TEXT_P(2));
	char *decoderString = text_to_cstring(PG_GETARG_TEXT_P(3));

	if (strcmp(decoderString, "auto") == 0)
	{
		/* TODO: look at the extension / content-encoding / content-type */
		decoderString = "csv";
	}

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	ByteSource byteSource;

	ReadBlockBlob(connectionString, containerName, path, &byteSource);

	TupleDecoder *decoder = BuildTupleDecoder(decoderString, tupleDescriptor,
											  &byteSource);

	DecodeTuplesIntoTupleStore(decoder, tupleStore);

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
	if (PG_ARGISNULL(4))
	{
		ereport(ERROR, (errmsg("decoder is required")));
	}

	char *connectionString = text_to_cstring(PG_GETARG_TEXT_P(0));
	char *containerName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char *path = text_to_cstring(PG_GETARG_TEXT_P(2));
	char *decoderString = text_to_cstring(PG_GETARG_TEXT_P(4));

	if (strcmp(decoderString, "auto") == 0)
	{
		/* TODO: look at the extension / content-encoding / content-type */
		decoderString = "csv";
	}

	Oid typeId = get_fn_expr_argtype(fcinfo->flinfo, 3);
	TupleDesc tupleDescriptor = TypeGetTupleDesc(typeId, NIL);
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	ByteSource byteSource;

	ReadBlockBlob(connectionString, containerName, path, &byteSource);

	TupleDecoder *decoder = BuildTupleDecoder(decoderString, tupleDescriptor,
											  &byteSource);

	DecodeTuplesIntoTupleStore(decoder, tupleStore);

	PG_RETURN_DATUM(0);
}


/*
 * DecodeTuplesIntoTupleStore gets all tuples from the decoder and writes them
 * to the given tuple store.
 */
static void
DecodeTuplesIntoTupleStore(TupleDecoder *decoder, Tuplestorestate *tupleStore)
{
	TupleDesc tupleDescriptor = decoder->tupleDescriptor;
	int columnCount = tupleDescriptor->natts;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));

	decoder->start(decoder->state);

	while (decoder->next(decoder->state, columnValues, columnNulls))
	{
		tuplestore_putvalues(tupleStore, tupleDescriptor, columnValues, columnNulls);

		CHECK_FOR_INTERRUPTS();
	}

	decoder->finish(decoder->state);
}
