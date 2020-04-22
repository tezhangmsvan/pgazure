/*-------------------------------------------------------------------------
 *
 * text_codec.h
 *	  Utilities for encoding and decoding stream of bytes into text
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef TEXT_CODEC_H
#define TEXT_CODEC_H


#include "access/tupdesc.h"
#include "pgazure/byte_io.h"
#include "pgazure/codecs.h"


TupleEncoder * CreateTextEncoder(ByteSink *byteSink, TupleDesc tupleDescriptor);
TupleDecoder * CreateTextDecoder(ByteSource *byteSource, TupleDesc tupleDescriptor);


#endif
