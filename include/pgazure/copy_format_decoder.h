/*-------------------------------------------------------------------------
 *
 * copy_format_decoderf.h
 *	  Utilities for decoding a stream of bytes using COPY infrastructure.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef COPY_FORMAT_DECODER_H
#define COPY_FORMAT_DECODER_H


#include "postgres.h"
#include "commands/copy.h"
#include "nodes/execnodes.h"
#include "pgazure/byte_io.h"
#include "pgazure/codecs.h"


typedef struct CopyFormatDecoderState
{
	CopyState copyState;
	ByteSource *byteSource;
	List *copyOptions;
	TupleDesc tupleDescriptor;
	EState *executorState;
} CopyFormatDecoderState;


TupleDecoder * CreateCopyFormatDecoder(ByteSource *byteSource,
                                       TupleDesc tupleDescriptor,
                                       List *copyOptions);
void CopyFormatDecoderStart(void *state);
bool CopyFormatDecoderNext(void *state, Datum *columnValues, bool *columnNulls);
void CopyFormatDecoderFinish(void *state);


#endif
