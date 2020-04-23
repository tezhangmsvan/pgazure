/*-------------------------------------------------------------------------
 *
 * zlib_compression.h
 *	  Utilities for compressing streams of bytes.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef ZLIB_COMPRESSION_H
#define ZLIB_COMPRESSION_H

#include "postgres.h"

#ifdef HAVE_LIBZ
#include "pgazure/byte_io.h"


ByteSink * CreateZLibCompressor(ByteSink *byteSink);
ByteSource * CreateZLibDecompressor(ByteSource *byteSource);


#endif
#endif
