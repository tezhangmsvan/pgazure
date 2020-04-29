/*-------------------------------------------------------------------------
 *
 * copy_format_decoder.c
 *		Implements functions that decode bytes to tuples in COPY format
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#include "commands/copy.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "pgazure/byte_io.h"
#include "pgazure/codecs.h"
#include "pgazure/copy_format_decoder.h"
#include "utils/builtins.h"
#include "utils/rel.h"


#if PG_VERSION_NUM >= 120000
#define NextCopyFromCompat NextCopyFrom
#else
#define NextCopyFromCompat(cstate, econtext, values, nulls) \
	NextCopyFrom(cstate, econtext, values, nulls, NULL)
#endif


/*
 * BeginCopyFrom takes a callback function to read bytes from, but it does not
 * allow you to specify an extra argument. Therefore we set this global variable
 * and use it the callback (ReadFromCurrentByteSource).
 */
static ByteSource *CurrentByteSource;


static int ReadFromCurrentByteSource(void *outBuf, int minRead, int maxRead);
static Relation StubRelation(TupleDesc tupleDescriptor);
static List * TupleDescColumnNameList(TupleDesc tupleDescriptor);
static List * ColumnNameListToCopyStmtAttributeList(List *columnNameList);


/*
 * CreateCopyFormatDecoder creates a tuple decoder that uses PostgreSQL's
 * COPY logic to parse the incoming bytes.
 */
TupleDecoder *
CreateCopyFormatDecoder(ByteSource *byteSource, TupleDesc tupleDescriptor,
						List *copyOptions)
{
	CopyFormatDecoderState *state = palloc0(sizeof(CopyFormatDecoderState));
	state->byteSource = byteSource;
	state->copyOptions = copyOptions;
	state->executorState = CreateExecutorState();
	state->tupleDescriptor = tupleDescriptor;

	TupleDecoder *decoder = CreateTupleDecoder(tupleDescriptor);
	decoder->state = state;
	decoder->start = CopyFormatDecoderStart;
	decoder->next = CopyFormatDecoderNext;
	decoder->finish = CopyFormatDecoderFinish;

	return decoder;
}


/*
 * CopyFormatDecoderStart start decoding bytes from the byte source.
 */
void
CopyFormatDecoderStart(void *state)
{
	CopyFormatDecoderState *decoder = (CopyFormatDecoderState *) state;

	/* set the global byte source to read in ReadFromCurrentByteSource */
	CurrentByteSource = decoder->byteSource;

	Relation stubRelation = StubRelation(decoder->tupleDescriptor);
	List *columnNameList = TupleDescColumnNameList(decoder->tupleDescriptor);
	List *attributeList = ColumnNameListToCopyStmtAttributeList(columnNameList);

	CopyState copyState = BeginCopyFrom(NULL, stubRelation, NULL, false,
	                                    ReadFromCurrentByteSource,
	                                    attributeList, decoder->copyOptions);

	decoder->copyState = copyState;
}


/*
 * CopyFormatDecoderNext reads a tuple in COPY format from the ByteSource and writes
 * the values to columnValues and columnNulls. If returns false when there are no
 * more tuples to read.
 */
bool
CopyFormatDecoderNext(void *state, Datum *columnValues, bool *columnNulls)
{
	CopyFormatDecoderState *decoder = (CopyFormatDecoderState *) state;
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


/*
 * ReadFromCurrentByteSource is a callback function passed to BeginCopyFrom to
 * read from CurrentByteSource.
 */
static int
ReadFromCurrentByteSource(void *outBuf, int minRead, int maxRead)
{
	int bytesRead = CurrentByteSource->read(CurrentByteSource->context,
	                                        outBuf, minRead, maxRead);

	return bytesRead;
}


/*
 * CopyFormatDecoderFinish cleans up state data structures and closes the copy source.
 */
void
CopyFormatDecoderFinish(void *state)
{
	CopyFormatDecoderState *decoder = (CopyFormatDecoderState *) state;

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
