/*-------------------------------------------------------------------------
 *
 * get_blob.c
 *     Implementation the blob_storage_get_blob UDFs
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "nodes/makefuncs.h"
#include "pgazure/blob_storage.h"
#include "pgazure/blob_storage_utils.h"
#include "pgazure/codecs.h"
#include "pgazure/compression.h"
#include "pgazure/copy_format_decoder.h"
#include "pgazure/set_returning_functions.h"
#include "pgazure/zlib_compression.h"
#include "utils/builtins.h"


static void ReadBlockBlobIntoTuplestore(char *connectionString, char *containerName,
                                        char *path, char *decoderString,
                                        char *compressionString,
                                        Tuplestorestate *tupleStore,
                                        TupleDesc tupleDescriptor);
static void DecodeTuplesIntoTupleStore(TupleDecoder *decoder,
                                       Tuplestorestate *tupleStore);


PG_FUNCTION_INFO_V1(blob_storage_get_blob);
PG_FUNCTION_INFO_V1(blob_storage_get_blob_anyelement);


/*
 * blob_storage_get_blob gets a blob from blob storage and tries to decode
 * it into a tuple store.
 *
 * The tuple is described by a list of output columns in the SQL query.
 */
Datum
blob_storage_get_blob(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("connection_string argument is required")));
	}
	if (PG_ARGISNULL(1))
	{
		ereport(ERROR, (errmsg("container_name argument is required")));
	}
	if (PG_ARGISNULL(2))
	{
		ereport(ERROR, (errmsg("path argument is required")));
	}
	if (PG_ARGISNULL(3))
	{
		ereport(ERROR, (errmsg("decoder argument is required")));
	}
	if (PG_ARGISNULL(4))
	{
		ereport(ERROR, (errmsg("compression argument is required")));
	}

	char *connectionString = text_to_cstring(PG_GETARG_TEXT_P(0));
	char *containerName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char *path = text_to_cstring(PG_GETARG_TEXT_P(2));
	char *decoderString = text_to_cstring(PG_GETARG_TEXT_P(3));
	char *compressionString = text_to_cstring(PG_GETARG_TEXT_P(4));

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	ReadBlockBlobIntoTuplestore(connectionString, containerName, path, decoderString,
	                            compressionString, tupleStore, tupleDescriptor);


	PG_RETURN_DATUM(0);
}


/*
 * blob_storage_get_blob gets a blob from blob storage and tries to decode
 * it into a tuple store.
 *
 * The tuple is described by a dummy argument with the same type as the tuple.
 */
Datum
blob_storage_get_blob_anyelement(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("connection_string argument is required")));
	}
	if (PG_ARGISNULL(1))
	{
		ereport(ERROR, (errmsg("container_name argument is required")));
	}
	if (PG_ARGISNULL(2))
	{
		ereport(ERROR, (errmsg("path argument is required")));
	}
	if (PG_ARGISNULL(4))
	{
		ereport(ERROR, (errmsg("decoder argument is required")));
	}
	if (PG_ARGISNULL(5))
	{
		ereport(ERROR, (errmsg("compression argument is required")));
	}

	char *connectionString = text_to_cstring(PG_GETARG_TEXT_P(0));
	char *containerName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char *path = text_to_cstring(PG_GETARG_TEXT_P(2));
	char *decoderString = text_to_cstring(PG_GETARG_TEXT_P(4));
	char *compressionString = text_to_cstring(PG_GETARG_TEXT_P(5));

	Oid typeId = get_fn_expr_argtype(fcinfo->flinfo, 3);
	TupleDesc tupleDescriptor = TypeGetTupleDesc(typeId, NIL);
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	ReadBlockBlobIntoTuplestore(connectionString, containerName, path, decoderString,
	                            compressionString, tupleStore, tupleDescriptor);

	PG_RETURN_DATUM(0);
}


/*
 * ReadBlockBlobIntoTuplestore reads a block blob from blob storage using the specified
 * connection string, container and path into a tuple store. The data is decoded using
 * the specified tuple decoder and compression strings.
 */
static void
ReadBlockBlobIntoTuplestore(char *connectionString, char *containerName, char *path,
                            char *decoderString, char *compressionString,
                            Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	ByteSource *byteSource = palloc0(sizeof(ByteSource));

	ReadBlockBlob(connectionString, containerName, path, byteSource);

	if (strcmp(compressionString, "auto") == 0)
	{
		/* TODO: look at the content-type / content-encoding */
		if (HasSuffix(path, ".gz"))
		{
			compressionString = "gzip";
		}
		else
		{
			compressionString = "none";
		}
	}

	byteSource = BuildDecompressor(compressionString, byteSource);

	if (strcmp(decoderString, "auto") == 0)
	{
		/* TODO: look at the content-type */
		decoderString = "csv";
	}

	TupleDecoder *decoder = BuildTupleDecoder(decoderString, tupleDescriptor,
											  byteSource);

	DecodeTuplesIntoTupleStore(decoder, tupleStore);

	pfree(byteSource);
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
