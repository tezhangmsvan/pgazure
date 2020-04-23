/*-------------------------------------------------------------------------
 *
 * zlib_compressor.c
 *     Compressor that uses libz to compress a stream of bytes as gzip.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgazure/byte_io.h"
#include "pgazure/zlib_compression.h"

#ifdef HAVE_LIBZ
#include <zlib.h>

#define Z_DEFAULT_COMPRESSION (-1)
/* Initial buffer sizes used in zlib compression. */
#define ZLIB_OUT_SIZE	65536
#define ZLIB_IN_SIZE	65536
#define DEFAULT_COMPRESSION_LEVEL 6
#define ZLIB_WINDOWSIZE 15
#define GZIP_ENCODING   16
#define ZLIB_CFACTOR    9


/*
 * ZLibCompressorState contains the internal state that is passed to
 * the write and close functions of the ByteSink.
 */
typedef struct ZLibCompressorState
{
    ByteSink *byteSink;

    z_streamp zp;
    char *zlibOut;
    size_t zlibOutSize;
} ZLibCompressorState;


static void ZLibWrite(void *context, void *buffer, int bytesToWrite);
static void ZLibClose(void *context);
static void DeflateBufferedData(ZLibCompressorState *state, bool flush);


/*
 * CreateZLibCompressor creates a ByteSink that compresses the bytes
 * that are written to it and writes the compressed bytes to another
 * ByteSink.
 */
ByteSink *
CreateZLibCompressor(ByteSink *byteSink)
{
	z_streamp zp = (z_streamp) palloc0(sizeof(z_stream));
	zp->zalloc = Z_NULL;
	zp->zfree = Z_NULL;
	zp->opaque = Z_NULL;

	ZLibCompressorState *state = palloc0(sizeof(ZLibCompressorState));
	state->byteSink = byteSink;
	state->zp = zp;

	/*
	 * zlibOutSize is the buffer size we tell zlib it can output to.  We
	 * actually allocate one extra byte because some routines want to append a
	 * trailing zero byte to the zlib output.
	 */
	state->zlibOut = (char *) palloc0(ZLIB_OUT_SIZE + 1);
	state->zlibOutSize = ZLIB_OUT_SIZE;

	if (deflateInit2(zp, DEFAULT_COMPRESSION_LEVEL, Z_DEFLATED,
					 ZLIB_WINDOWSIZE | GZIP_ENCODING, ZLIB_CFACTOR,
					 Z_DEFAULT_STRATEGY) != Z_OK)
	{
		ereport(ERROR, (errmsg("could not initialize compression library: %s", zp->msg)));
	}

	/* Just be paranoid - maybe End is called after Start, with no Write */
	zp->next_out = (void *) state->zlibOut;
	zp->avail_out = state->zlibOutSize;

	ByteSink *compressor = palloc0(sizeof(ByteSink));
	compressor->context = state;
	compressor->write = ZLibWrite;
	compressor->close = ZLibClose;

	return compressor;
}


/*
 * ZLibWrite appends the given buffer to the zlib buffer for compression.
 */
static void
ZLibWrite(void *context, void *buffer, int bytesToWrite)
{
	ZLibCompressorState *state = (ZLibCompressorState *) context;

	state->zp->next_in = buffer;
	state->zp->avail_in = bytesToWrite;

	DeflateBufferedData(state, false);
}


/*
 * ZLibClose flushes the data that remains in the zlib buffer and closes
 */
static void
ZLibClose(void *context)
{
	ZLibCompressorState *state = (ZLibCompressorState *) context;
	ByteSink *byteSink = state->byteSink;
	z_streamp zp = state->zp;

	zp->next_in = NULL;
	zp->avail_in = 0;

	/* Flush any remaining data from zlib buffer */
	DeflateBufferedData(state, true);

	if (deflateEnd(zp) != Z_OK)
	{
		ereport(ERROR, (errmsg("could not close compression stream: %s", zp->msg)));
	}

	byteSink->close(byteSink->context);

	pfree(state->zlibOut);
	pfree(state->zp);
	pfree(state);
}


/*
 * DeflateBufferedData compresses the data in the zlib buffer and writes
 * the compressed data to the byteSink.
 *
 * Based on compress_io.c in PostgreSQL.
 */
static void
DeflateBufferedData(ZLibCompressorState *state, bool flush)
{
	z_streamp zp = state->zp;
	ByteSink *byteSink = state->byteSink;

	while (state->zp->avail_in != 0 || flush)
	{
		int res = deflate(zp, flush ? Z_FINISH : Z_NO_FLUSH);
		if (res == Z_STREAM_ERROR)
		{
			ereport(ERROR, (errmsg("could not compress data: %s", zp->msg)));
		}

		if ((flush && (zp->avail_out < state->zlibOutSize))
			|| (zp->avail_out == 0) || (zp->avail_in != 0))
		{
			/*
			 * Extra paranoia: avoid zero-length chunks, since a zero length
			 * chunk is the EOF marker in the custom format. This should never
			 * happen but...
			 */
			if (zp->avail_out < state->zlibOutSize)
			{
				/*
				 * Any write function should do its own error checking but to
				 * make sure we do a check here as well...
				 */
				size_t len = state->zlibOutSize - zp->avail_out;

				byteSink->write(byteSink->context, state->zlibOut, len);
			}
			zp->next_out = (void *) state->zlibOut;
			zp->avail_out = state->zlibOutSize;
		}

		if (res == Z_STREAM_END)
		{
			break;
		}
	}
}

#endif
