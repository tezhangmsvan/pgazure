/*-------------------------------------------------------------------------
 *
 * compression.h
 *	  Utilities for compressing streams of bytes.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef COMPRESSION_H
#define COMPRESSION_H

#include "postgres.h"
#include "pgazure/byte_io.h"


/*
 * CompressionType represents different ways in which PGAzure can compress
 * and decompress byte streams.
 */
typedef enum CompressionType
{
#ifdef HAVE_LIBZ
	/* in a HAVE_LIBZ block to make sure we don't use it otherwise */
	COMPRESSION_GZIP,
#endif
	COMPRESSION_NONE
} CompressionType;


ByteSink * BuildCompressor(char *compressionString, ByteSink *byteSink);
ByteSource * BuildDecompressor(char *compressionString, ByteSource *byteSource);


#endif
