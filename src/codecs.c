#include "postgres.h"

#include "access/tupdesc.h"
#include "pgazure/byte_io.h"
#include "pgazure/codecs.h"
#include "pgazure/copy_format_decoder.h"
#include "pgazure/copy_format_encoder.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"


static TupleCodecType TupleCodecTypeFromString(char *string);


/*
 * CreateTupleEncoder creates an empty tuple encoder.
 *
 * The state and functions still need to be set.
 */
TupleEncoder *
CreateTupleEncoder(TupleDesc tupleDescriptor)
{
	TupleEncoder *encoder = palloc0(sizeof(TupleEncoder));
	encoder->tupleDescriptor = tupleDescriptor;
	return encoder;
}


/*
 * CreateTupleDecoder creates an empty tuple decoder.
 *
 * The state and functions still need to be set.
 */
TupleDecoder *
CreateTupleDecoder(TupleDesc tupleDescriptor)
{
	TupleDecoder *decoder = palloc0(sizeof(TupleDecoder));
	decoder->tupleDescriptor = tupleDescriptor;
	return decoder;
}

/*
 * BuildTupleEncoder builds a tuple encoder from a string.
 */
TupleEncoder *
BuildTupleEncoder(char *encoderString, TupleDesc tupleDescriptor, ByteSink *byteSink)
{
	TupleEncoder *encoder = NULL;
	TupleCodecType codecType = TupleCodecTypeFromString(encoderString);

	switch (codecType)
	{
		case TUPLE_CODEC_BINARY:
		case TUPLE_CODEC_CSV:
		case TUPLE_CODEC_TEXT:
		{
			DefElem *formatResultOption =
				makeDefElem("format", (Node *) makeString(encoderString), -1);
			List *copyOptions = list_make1(formatResultOption);

			encoder = CreateCopyFormatEncoder(byteSink, tupleDescriptor, copyOptions);
		}

		case TUPLE_CODEC_JSON:
		{

		}
	}

	return encoder;
}


/*
 * BuildTupleEncoder builds a tuple decoder from a string.
 */
TupleDecoder *
BuildTupleDecoder(char *decoderString, TupleDesc tupleDescriptor, ByteSource *byteSource)
{
	TupleDecoder *decoder = NULL;
	TupleCodecType codecType = TupleCodecTypeFromString(decoderString);

	switch (codecType)
	{
		case TUPLE_CODEC_BINARY:
		case TUPLE_CODEC_CSV:
		case TUPLE_CODEC_TEXT:
		{
			DefElem *formatResultOption =
				makeDefElem("format", (Node *) makeString(decoderString), -1);
			List *copyOptions = list_make1(formatResultOption);

			decoder = CreateCopyFormatDecoder(byteSource, tupleDescriptor, copyOptions);
		}

		case TUPLE_CODEC_JSON:
		{

		}
	}

	return decoder;
}


/*
 * TupleCodecTypeFromString determines the codec type in an encoder/decoder
 * string.
 */
static TupleCodecType
TupleCodecTypeFromString(char *codecString)
{
	if (strcmp(codecString, "csv") != 0)
	{
		return TUPLE_CODEC_CSV;
	}
	else if (strcmp(codecString, "text") != 0)
	{
		return TUPLE_CODEC_TEXT;
	}
	else if (strcmp(codecString, "binary") != 0)
	{
		return TUPLE_CODEC_BINARY;
	}
	else if (strcmp(codecString, "json") != 0)
	{
		return TUPLE_CODEC_JSON;
	}
	else
	{
		ereport(ERROR, (errmsg("invalid decoder: %s", codecString)));
	}
}
