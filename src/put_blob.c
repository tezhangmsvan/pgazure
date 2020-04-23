#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "nodes/makefuncs.h"
#include "pgazure/blob_storage.h"
#include "pgazure/blob_storage_utils.h"
#include "pgazure/byte_io.h"
#include "pgazure/codecs.h"
#include "pgazure/compression.h"
#include "pgazure/zlib_compression.h"
#include "storage/itemptr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/typcache.h"


PG_FUNCTION_INFO_V1(blob_storage_put_blob_sfunc);
PG_FUNCTION_INFO_V1(blob_storage_put_blob_final);


typedef struct
{
	TupleDesc tupleDescriptor;
	FmgrInfo *columnOutputFunctions;
	MemoryContext rowContext;
	Datum *values;
	bool *nulls;
	TupleEncoder *encoder;
} BlobStoragePutBlobState;


Datum
blob_storage_put_blob_sfunc(PG_FUNCTION_ARGS)
{
	BlobStoragePutBlobState *aggregateState =
		(BlobStoragePutBlobState *) PG_GETARG_POINTER(0);
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(4);

	if (aggregateState == NULL)
	{
		MemoryContext aggContext;
		MemoryContext oldContext;

		char *connectionString = text_to_cstring(PG_GETARG_TEXT_P(1));
		char *containerName = text_to_cstring(PG_GETARG_TEXT_P(2));
		char *path = text_to_cstring(PG_GETARG_TEXT_P(3));
		char *encoderString = "csv";
		char *compressionString = "auto";

		if (PG_NARGS() > 5)
		{
			encoderString = text_to_cstring(PG_GETARG_TEXT_P(5));
		}

		if (PG_NARGS() > 6)
		{
			compressionString = text_to_cstring(PG_GETARG_TEXT_P(6));
		}

		if (!AggCheckCallContext(fcinfo, &aggContext))
		{
			elog(ERROR, "aggregate function called in non-aggregate context");
		}

		oldContext = MemoryContextSwitchTo(aggContext);
		aggregateState = palloc0(sizeof(BlobStoragePutBlobState));

		Oid tupType = HeapTupleHeaderGetTypeId(rec);
		int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
		aggregateState->tupleDescriptor = lookup_rowtype_tupdesc(tupType, tupTypmod);

		int natts = aggregateState->tupleDescriptor->natts;
		aggregateState->values = palloc(natts * sizeof(Datum));
		aggregateState->nulls = (bool *) palloc(natts * sizeof(bool));

		ByteSink *byteSink = palloc0(sizeof(ByteSink));
		WriteBlockBlob(connectionString, containerName, path, byteSink);

		if (strcmp(compressionString, "auto") == 0)
		{
			if (HasSuffix(path, ".gz"))
			{
				compressionString = "gzip";
			}
			else
			{
				compressionString = "none";
			}
		}

		byteSink = BuildCompressor(compressionString, byteSink);

		TupleEncoder *encoder = BuildTupleEncoder(encoderString,
												  aggregateState->tupleDescriptor,
		                                          byteSink);

		encoder->start(encoder->state);

		aggregateState->encoder = encoder;

		MemoryContextSwitchTo(oldContext);
	}

	HeapTupleData tuple;
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;

	TupleDesc tupleDesc = aggregateState->tupleDescriptor;
	Datum *values = aggregateState->values;
	bool *nulls = aggregateState->nulls;
	heap_deform_tuple(&tuple, tupleDesc, values, nulls);

	aggregateState->encoder->push(aggregateState->encoder->state, values, nulls);

	PG_RETURN_POINTER(aggregateState);
}


Datum
blob_storage_put_blob_final(PG_FUNCTION_ARGS)
{
	BlobStoragePutBlobState *aggregateState =
		(BlobStoragePutBlobState *) PG_GETARG_POINTER(0);
	TupleEncoder *encoder = aggregateState->encoder;

	encoder->finish(encoder->state);

	ReleaseTupleDesc(aggregateState->tupleDescriptor);

	PG_RETURN_VOID();
}
