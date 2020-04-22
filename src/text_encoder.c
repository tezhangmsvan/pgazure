#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "lib/stringinfo.h"
#include "pgazure/byte_io.h"
#include "pgazure/codecs.h"
#include "pgazure/copy_utils.h"
#include "pgazure/text_codec.h"
#include "utils/builtins.h"


typedef struct TextEncoderState
{
	ByteSink *byteSink;
	FmgrInfo *columnOutputFunctions;
} TextEncoderState;


static void AppendValueText(StringInfo buffer, FmgrInfo *columnOutputFunctions,
                            Datum *valueArray, bool *isNullArray);
static void TextEncoderStart(void *state);
static void TextEncoderPush(void *state, Datum *columnValues, bool *columnNulls);
static void TextEncoderFinish(void *state);


TupleEncoder *
CreateTextEncoder(ByteSink *byteSink, TupleDesc tupleDescriptor)
{
	if (tupleDescriptor->natts != 1)
	{
		ereport(ERROR, (errmsg("can only use text encoder with a single column")));
	}

	TextEncoderState *state = palloc0(sizeof(TextEncoderState));
	state->byteSink = byteSink;
	state->columnOutputFunctions = ColumnOutputFunctions(tupleDescriptor, false);

	TupleEncoder *encoder = CreateTupleEncoder(tupleDescriptor);
	encoder->state = state;
	encoder->start = TextEncoderStart;
	encoder->push = TextEncoderPush;
	encoder->finish = TextEncoderFinish;

	return encoder;
}


static void
TextEncoderStart(void *state)
{
	/* nothing to do */
}


static void
TextEncoderPush(void *state, Datum *columnValues, bool *columnNulls)
{
	TextEncoderState *encoder = (TextEncoderState *) state;
	ByteSink *byteSink = encoder->byteSink;
	FmgrInfo *columnOutputFunctions = encoder->columnOutputFunctions;
	StringInfo buffer = makeStringInfo();

    AppendValueText(buffer, columnOutputFunctions, columnValues, columnNulls);

	byteSink->write(byteSink->context, buffer->data, buffer->len);
}


static void
TextEncoderFinish(void *state)
{
	TextEncoderState *encoder = (TextEncoderState *) state;
	ByteSink *byteSink = encoder->byteSink;

	byteSink->close(byteSink->context);
}


/*
 * AppendValueText calls the output function for the Datum pointed to by valueArray
 * and appends the text to buffer. If the value is NULL, the nothing is appended.
 */
static void
AppendValueText(StringInfo buffer, FmgrInfo *columnOutputFunctions, Datum *valueArray,
				bool *isNullArray)
{
	int columnIndex = 0;
	Datum value = valueArray[columnIndex];
	bool isNull = isNullArray[columnIndex];

	if (!isNull)
	{
		FmgrInfo *outputFunctionPointer = &columnOutputFunctions[columnIndex];
		char *valueText = OutputFunctionCall(outputFunctionPointer, value);

		appendStringInfoString(buffer, valueText);
	}
}
