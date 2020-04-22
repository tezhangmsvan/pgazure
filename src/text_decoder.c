#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "lib/stringinfo.h"
#include "pgazure/byte_io.h"
#include "pgazure/codecs.h"
#include "pgazure/text_codec.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


typedef struct TextDecoderState
{
	ByteSource *byteSource;
	bool valueReturned;
	Oid functionId;
	Oid typeIOParam;
} TextDecoderState;


static void TextDecoderStart(void *state);
static bool TextDecoderNext(void *state, Datum *columnValues, bool *columnNulls);
static void TextDecoderFinish(void *state);


TupleDecoder *
CreateTextDecoder(ByteSource *byteSource, TupleDesc tupleDescriptor)
{
	if (tupleDescriptor->natts != 1)
	{
		ereport(ERROR, (errmsg("can only use text encoder with a single column")));
	}

	TextDecoderState *state = palloc0(sizeof(TextDecoderState));
	state->byteSource = byteSource;
	state->valueReturned = false;

	Form_pg_attribute attr = TupleDescAttr(tupleDescriptor, 0);
	Oid valueTypeId = attr->atttypid;

	getTypeInputInfo(valueTypeId, &state->functionId, &state->typeIOParam);

	TupleDecoder *decoder = CreateTupleDecoder(tupleDescriptor);
	decoder->state = state;
	decoder->start = TextDecoderStart;
	decoder->next = TextDecoderNext;
	decoder->finish = TextDecoderFinish;

	return decoder;
}


void
TextDecoderStart(void *state)
{
	/* nothing to do */
}


/*
 * TextDecoderNext
 */
bool
TextDecoderNext(void *state, Datum *columnValues, bool *columnNulls)
{
	TextDecoderState *decoder = (TextDecoderState *) state;

	if (decoder->valueReturned)
	{
		/* this decoder can only return one value */
		return false;
	}

	ByteSource* byteSource = decoder->byteSource;
	StringInfo text = makeStringInfo();
	char buffer[65536];
	int bytesRead = 0;

	do
	{
		bytesRead = byteSource->read(byteSource->context, buffer, 0, 65536);

		for (int byteIndex = 0; byteIndex < bytesRead; byteIndex++)
		{
			appendStringInfoCharMacro(text, buffer[byteIndex]);
		}

		CHECK_FOR_INTERRUPTS();
	}
	while (bytesRead > 0);

	if (text->len == 0)
	{
		columnNulls[0] = true;
	}
	else
	{
		columnNulls[0] = false;
		columnValues[0] = OidInputFunctionCall(decoder->functionId, text->data,
		                                       decoder->typeIOParam, -1);
	}

	decoder->valueReturned = true;

	return true;
}


void
TextDecoderFinish(void *state)
{
	TextDecoderState *decoder = (TextDecoderState *) state;
	ByteSource* byteSource = decoder->byteSource;

	byteSource->close(byteSource->context);
}
