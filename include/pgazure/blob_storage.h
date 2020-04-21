/*-------------------------------------------------------------------------
 *
 * blob_storage.h
 *	  Definitions of functions for interacting with blob storage
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef BLOB_STORAGE_H
#define BLOB_STORAGE_H
#ifdef __cplusplus
extern "C" {
#endif


#include "pgazure/byte_io.h"


typedef struct CloudBlob
{
	const char *name;
	size_t size;

	const char *contentLanguage;
	const char *contentLength;
	const char *contentEncoding;
	const char *contentType;
	const char *contentMD5;
	const char *etag;
	const char *lastModified;
} CloudBlob;


void ReadBlockBlob(char *connectionString, char *containerName, char *path, ByteSource *byteSource);
void WriteBlockBlob(char *connectionString, char *containerName, char *path, ByteSink *byteSink);
void ListBlobs(char *connectionString, char *containerName, char *prefix, void (*processBlob)(void *, CloudBlob *), void *processBlobContext);

#ifdef __cplusplus
}
#endif
#endif
