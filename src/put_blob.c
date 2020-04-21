#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "nodes/makefuncs.h"
#include "pgazure/blob_storage.h"
#include "pgazure/byte_io.h"
#include "pgazure/copy_format_encoder.h"
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
	CopyFormatEncoder *encoder;
	ByteSink *byteSink;
} BlobStoragePutBlobState;


Datum
blob_storage_put_blob_sfunc(PG_FUNCTION_ARGS)
{
	BlobStoragePutBlobState *state =
		(BlobStoragePutBlobState *) PG_GETARG_POINTER(0);
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(4);

	if (state == NULL)
	{
		MemoryContext agg_context;
		MemoryContext old_context;

		char *connectionString = text_to_cstring(PG_GETARG_TEXT_P(1));
		char *containerName = text_to_cstring(PG_GETARG_TEXT_P(2));
		char *path = text_to_cstring(PG_GETARG_TEXT_P(3));

		if (!AggCheckCallContext(fcinfo, &agg_context))
		{
			elog(ERROR, "aggregate function called in non-aggregate context");
		}

		old_context = MemoryContextSwitchTo(agg_context);
		state = palloc0(sizeof(BlobStoragePutBlobState));

		Oid tupType = HeapTupleHeaderGetTypeId(rec);
		int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
		state->tupleDescriptor = lookup_rowtype_tupdesc(tupType, tupTypmod);

		int natts = state->tupleDescriptor->natts;
		state->values = palloc(natts * sizeof(Datum));
		state->nulls = (bool *) palloc(natts * sizeof(bool));

		ByteSink *byteSink = palloc0(sizeof(ByteSink));
		WriteBlockBlob(connectionString, containerName, path, byteSink);

		DefElem *formatResultOption = makeDefElem("format", (Node *) makeString("csv"), -1);
		List *copyOptions = list_make1(formatResultOption);

		state->encoder = CopyFormatEncoderCreate(byteSink, state->tupleDescriptor,
		                                         copyOptions);
		CopyFormatEncoderStart(state->encoder);

		state->byteSink = byteSink;

		MemoryContextSwitchTo(old_context);
	}

	HeapTupleData tuple;
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;

	TupleDesc tupleDesc = state->tupleDescriptor;
	Datum *values = state->values;
	bool *nulls = state->nulls;
	heap_deform_tuple(&tuple, tupleDesc, values, nulls);

	CopyFormatEncoderPush(state->encoder, values, nulls);

	PG_RETURN_POINTER(state);
}


Datum
blob_storage_put_blob_final(PG_FUNCTION_ARGS)
{
	BlobStoragePutBlobState *state =
		(BlobStoragePutBlobState *) PG_GETARG_POINTER(0);

	CopyFormatEncoderFinish(state->encoder);

	ReleaseTupleDesc(state->tupleDescriptor);

	PG_RETURN_VOID();
}
