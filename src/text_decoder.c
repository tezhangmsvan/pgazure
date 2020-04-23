/*-------------------------------------------------------------------------
 *
 * text_decoder.c
 *     Tuple decoder that turns the entire input into a single value.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "lib/stringinfo.h"
#include "pgazure/byte_io.h"
#include "pgazure/codecs.h"
#include "pgazure/text_codec.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/*
 * TextDecoderState contains the internal state that is passed to the
 * decoder functions.
 */
typedef struct TextDecoderState
{
	/* the decoder reads from this byte source */
	ByteSource *byteSource;

	/*
	 * valueReturned is true when a value has been returned, such that the next
	 * function returns false the second time.
	 */
	bool valueReturned;

	/* OID of the input function used to parse the source data */
	Oid inputFunctionId;
	Oid typeIOParam;
} TextDecoderState;


static void TextDecoderStart(void *state);
static bool TextDecoderNext(void *state, Datum *columnValues, bool *columnNulls);
static void TextDecoderFinish(void *state);


/*
 * CreateTextDecoder creates a tuple decoder that reads all bytes from the
 * byte source and parses it via the input function of the only column in
 * the TupleDesc to produce a single value. This is useful for decoding files
 * which contain a single JSON object.
 */
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

	getTypeInputInfo(valueTypeId, &state->inputFunctionId, &state->typeIOParam);

	TupleDecoder *decoder = CreateTupleDecoder(tupleDescriptor);
	decoder->state = state;
	decoder->start = TextDecoderStart;
	decoder->next = TextDecoderNext;
	decoder->finish = TextDecoderFinish;

	return decoder;
}


/*
 * TextDecoderStart is a noop to satisfy the tuple decoder API.
 */
void
TextDecoderStart(void *state)
{
	/* nothing to do */
}


/*
 * TextDecoderNext reads all bytes from the byte source and parses them
 * by calling the input function of the tuple.
 */
bool
TextDecoderNext(void *state, Datum *columnValues, bool *columnNulls)
{
	TextDecoderState *decoder = (TextDecoderState *) state;

	if (decoder->valueReturned)
	{
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
		columnValues[0] = OidInputFunctionCall(decoder->inputFunctionId, text->data,
		                                       decoder->typeIOParam, -1);
	}

	/* there will not be a second row */
	decoder->valueReturned = true;

	return true;
}


/*
 * TextDecoderFinish closes the byte source of the decoder.
 */
void
TextDecoderFinish(void *state)
{
	TextDecoderState *decoder = (TextDecoderState *) state;
	ByteSource* byteSource = decoder->byteSource;

	byteSource->close(byteSource->context);
}
