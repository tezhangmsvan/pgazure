/*-------------------------------------------------------------------------
 *
 * codecs.h
 *	  Definition of tuple encode/decode data structures.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef CODECS_H
#define CODECS_H


#include "access/tupdesc.h"
#include "pgazure/byte_io.h"


/*
 * TupleCodecType represents different ways in which PGAzure can encode
 * and decode tuples.
 */
typedef enum TupleCodecType
{
	TUPLE_CODEC_CSV,
	TUPLE_CODEC_TEXT,
	TUPLE_CODEC_BINARY,
	TUPLE_CODEC_JSON,
} TupleCodecType;

/*
 * TupleEncoder represents the mechanism for encoding tuples.
 */
typedef struct TupleEncoder
{
	TupleCodecType type;
	TupleDesc tupleDescriptor;

	void *state;

	void (*start) (void *state);
	void (*push) (void *state, Datum *columnValues, bool *columnNulls);
	void (*finish) (void *state);

} TupleEncoder;

/*
 * TupleDecoder represents the mechanism for decoding tuples.
 */
typedef struct TupleDecoder
{
	TupleDesc tupleDescriptor;

	void *state;

	void (*start) (void *state);
	bool (*next) (void *state, Datum *columnValues, bool *columnNulls);
	void (*finish) (void *state);

} TupleDecoder;


TupleEncoder * CreateTupleEncoder(TupleDesc tupleDescriptor);
TupleDecoder * CreateTupleDecoder(TupleDesc tupleDescriptor);

TupleEncoder * BuildTupleEncoder(char *encoderString, TupleDesc tupleDescriptor,
								 ByteSink *byteSink);
TupleDecoder * BuildTupleDecoder(char *decoderString, TupleDesc tupleDescriptor,
								 ByteSource *byteSource);


#endif
