/*-------------------------------------------------------------------------
 *
 * text_decoder.h
 *	  Utilities for decoding a stream of bytes into a text field.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef TEXT_DECODER_H
#define TEXT_DECODER_H


#include "postgres.h"
#include "pgazure/byte_io.h"
#include "pgazure/codecs.h"


typedef struct TextDecoderState
{
	TupleDesc tupleDescriptor;
	ByteSource *byteSource;
	StringInfo *buffer;
} TextDecoderState;


TupleDecoder * CreateTextDecoder(ByteSource *byteSource, TupleDesc tupleDescriptor);
void TextDecoderStart(void *state);
bool TextDecoderNext(void *state, Datum *columnValues, bool *columnNulls);
void TextDecoderFinish(void *state);


#endif
