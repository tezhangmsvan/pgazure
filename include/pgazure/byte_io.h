/*-------------------------------------------------------------------------
 *
 * byte_io.h
 *	  Binary I/O via function calls
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef BYTE_IO_H
#define BYTE_IO_H
#ifdef __cplusplus
extern "C" {
#endif


typedef struct ByteSource
{
	void *context;
	int (*read) (void *context, void *outbuf, int minread, int maxread);
	void (*close) (void *context);
} ByteSource;

typedef struct ByteSink
{
	void *context;
	void *context2;
	int (*write) (void *context, void *outbuf, int numBytes);
	void (*close) (void *context2);
} ByteSink;


#ifdef __cplusplus
}
#endif
#endif
