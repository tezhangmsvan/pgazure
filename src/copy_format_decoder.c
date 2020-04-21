#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "commands/copy.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "pgazure/byte_io.h"
#include "pgazure/copy_format_decoder.h"
#include "utils/builtins.h"
#include "utils/rel.h"


#if PG_VERSION_NUM >= 120000
#define NextCopyFromCompat NextCopyFrom
#else
#define NextCopyFromCompat(cstate, econtext, values, nulls) \
	NextCopyFrom(cstate, econtext, values, nulls, NULL)
#endif


static ByteSource *CurrentByteSource;


static int ReadFromCurrentByteSource(void *outBuf, int minRead, int maxRead);
static Relation StubRelation(TupleDesc tupleDescriptor);
static List * TupleDescColumnNameList(TupleDesc tupleDescriptor);
static List * ColumnNameListToCopyStmtAttributeList(List *columnNameList);


void
DecodeCopyFormatByteSourceIntoTupleStore(ByteSource *byteSource,
										 Tuplestorestate *tupleStore,
										 TupleDesc tupleDescriptor,
										 List *copyOptions)
{
	int columnCount = tupleDescriptor->natts;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));

	CopyFormatDecoder *decoder = CopyFormatDecoderCreate(byteSource, tupleDescriptor,
	                                                     copyOptions);

	CopyFormatDecoderStart(decoder);

	while (CopyFormatDecoderNext(decoder, columnValues, columnNulls))
	{
		tuplestore_putvalues(tupleStore, tupleDescriptor, columnValues, columnNulls);

		CHECK_FOR_INTERRUPTS();
	}

	CopyFormatDecoderFinish(decoder);
}


CopyFormatDecoder *
CopyFormatDecoderCreate(ByteSource *byteSource, TupleDesc tupleDescriptor,
						List *copyOptions)
{
	CopyFormatDecoder *decoder = palloc0(sizeof(CopyFormatDecoder));
	decoder->byteSource = byteSource;
	decoder->copyOptions = copyOptions;
	decoder->executorState = CreateExecutorState();
	decoder->tupleDescriptor = tupleDescriptor;
	return decoder;
}


void
CopyFormatDecoderStart(CopyFormatDecoder *decoder)
{
	CurrentByteSource = decoder->byteSource;

	Relation stubRelation = StubRelation(decoder->tupleDescriptor);
	List *columnNameList = TupleDescColumnNameList(decoder->tupleDescriptor);
	List *attributeList = ColumnNameListToCopyStmtAttributeList(columnNameList);

	CopyState copyState = BeginCopyFrom(NULL, stubRelation, NULL, false,
	                                    ReadFromCurrentByteSource,
	                                    attributeList, decoder->copyOptions);

	decoder->copyState = copyState;
}


bool
CopyFormatDecoderNext(CopyFormatDecoder *decoder, Datum *columnValues, bool *columnNulls)
{
	EState *executorState = decoder->executorState;
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	ExprContext *executorExpressionContext = GetPerTupleExprContext(executorState);
	
	ResetPerTupleExprContext(executorState);
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	CopyState copyState = decoder->copyState;

    /* set up callback to identify error line number */
	ErrorContextCallback errorCallback;
    errorCallback.callback = CopyFromErrorCallback;
    errorCallback.arg = (void *) copyState;
    errorCallback.previous = error_context_stack;
    error_context_stack = &errorCallback;

	bool nextRowFound = NextCopyFromCompat(copyState, executorExpressionContext,
									       columnValues, columnNulls);

   	error_context_stack = errorCallback.previous;
	MemoryContextSwitchTo(oldContext);
	return nextRowFound;
}


static int
ReadFromCurrentByteSource(void *outBuf, int minRead, int maxRead)
{
	int bytesRead = CurrentByteSource->read(CurrentByteSource->context,
	                                        outBuf, minRead, maxRead);

	return bytesRead;
}


void
CopyFormatDecoderFinish(CopyFormatDecoder *decoder)
{
	EndCopyFrom(decoder->copyState);

	CurrentByteSource->close(CurrentByteSource->context);

	CurrentByteSource = NULL;
}


/*
 * StubRelation creates a stub Relation from the given tuple descriptor.
 * To be able to use copy.c, we need a Relation descriptor. As there is no
 * relation corresponding to the data loaded from workers, we need to fake one.
 * We just need the bare minimal set of fields accessed by BeginCopyFrom().
 */
static Relation
StubRelation(TupleDesc tupleDescriptor)
{
	Relation stubRelation = palloc0(sizeof(RelationData));
	stubRelation->rd_att = tupleDescriptor;
	stubRelation->rd_rel = palloc0(sizeof(FormData_pg_class));
	stubRelation->rd_rel->relkind = RELKIND_RELATION;

	return stubRelation;
}


/*
 * TupleDescColumnNameList returns a list of column names (char *) for a TupleDesc.
 */
static List *
TupleDescColumnNameList(TupleDesc tupleDescriptor)
{
	int columnCount = tupleDescriptor->natts;
	List *columnNameList = NIL;

	/* build the list of column names for remote COPY statements */
	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescriptor, columnIndex);
		char *columnName = NameStr(currentColumn->attname);

		if (currentColumn->attisdropped
#if PG_VERSION_NUM >= 120000
			|| currentColumn->attgenerated == ATTRIBUTE_GENERATED_STORED
#endif
			)
		{
			continue;
		}

		columnNameList = lappend(columnNameList, columnName);
	}

	return columnNameList;
}


/*
 * ColumnNameListToCopyStmtAttributeList converts a list of column names (char *) to
 * an attribute list (Value *) that can be used in a CopyStmt.
 */
static List *
ColumnNameListToCopyStmtAttributeList(List *columnNameList)
{
	List *attributeList = NIL;
	ListCell *columnNameCell = NULL;

    /* wrap the column names as Values */
    foreach(columnNameCell, columnNameList)
    {   
        char *columnName = (char *) lfirst(columnNameCell);
        Value *columnNameValue = makeString(columnName);
        
        attributeList = lappend(attributeList, columnNameValue);
    }

	return attributeList;
}
