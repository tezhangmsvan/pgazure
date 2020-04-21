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


typedef struct CopyFormatDecoder
{
	CopyState copyState;
	ByteSource *byteSource;
	List *copyOptions;
	TupleDesc tupleDescriptor;
	EState *executorState;
} CopyFormatDecoder;


void DecodeCopyFormatByteSourceIntoTupleStore(ByteSource *byteSource,
                                              Tuplestorestate *tupleStore,
                                              TupleDesc tupleDescriptor,
                                              List *copyOptions);
CopyFormatDecoder * CopyFormatDecoderCreate(ByteSource *byteSource,
											TupleDesc tupleDescriptor,
											List *copyOptions);
void CopyFormatDecoderStart(CopyFormatDecoder *decoder);
bool CopyFormatDecoderNext(CopyFormatDecoder *decoder, Datum *columnValues,
                           bool *columnNulls);
void CopyFormatDecoderFinish(CopyFormatDecoder *decoder);


#endif
