#include "postgres.h"
#include "miscadmin.h"

#include "pgazure/byte_io.h"
#include "pgazure/zlib_compression.h"

#ifdef HAVE_LIBZ
#include <zlib.h>

#define ZLIB_OUT_SIZE	65536
#define ZLIB_IN_SIZE	65536
#define DEFAULT_COMPRESSION_LEVEL 6
#define ZLIB_WINDOWSIZE 15
#define GZIP_DECODING   32
#define ZLIB_CFACTOR    9


typedef struct ZLibDecompressorState
{
    z_streamp zp;

    ByteSource *byteSource;

	char *inputBuffer;
	bool endOfInputReached;

	char *outputBuffer;

	/* up to which byte in the output buffer we've consumed */
	int outputBufferOffset;
} ZLibDecompressorState;


static int ZLibDecompressorRead(void *context, void *buffer, int minRead, int maxRead);
static void ZLibDecompressorClose(void *context);
static bool IsDecompressionDone(ZLibDecompressorState *state);
static int FlushOutputBufferIntoBuffer(ZLibDecompressorState *state,
                                       char *buffer, int bufferOffset, int maxRead);
static void FillInputBufferFromSource(ZLibDecompressorState *state);
static void DecompressInputBufferIntoOutputBuffer(ZLibDecompressorState *state);


/*
 * CreateZLibDecompressor creates a ByteSource that decompresses the bytes
 * coming in from another ByteSource.
 */
ByteSource *
CreateZLibDecompressor(ByteSource *byteSource)
{
	z_streamp zp = (z_streamp) palloc0(sizeof(z_stream));
	zp->zalloc = Z_NULL;
	zp->zfree = Z_NULL;
	zp->opaque = Z_NULL;
	zp->avail_in = 0;
	zp->avail_out = ZLIB_OUT_SIZE;

	ZLibDecompressorState *state = palloc0(sizeof(ZLibDecompressorState));
	state->byteSource = byteSource;
	state->inputBuffer = palloc0(ZLIB_IN_SIZE);
	state->outputBuffer = palloc0(ZLIB_OUT_SIZE + 1);
	state->outputBufferOffset = 0;
	state->zp = zp;

	if (inflateInit2(zp, ZLIB_WINDOWSIZE | GZIP_DECODING) != Z_OK)
	{
		ereport(ERROR, (errmsg("could not initialize compression library: %s", zp->msg)));
	}

	ByteSource *compressor = palloc0(sizeof(ByteSource));
	compressor->context = state;
	compressor->read = ZLibDecompressorRead;
	compressor->close = ZLibDecompressorClose;

	return compressor;
}


/*
 * ZLibDecompressorRead reads bytes of decompressed data.
 */
static int
ZLibDecompressorRead(void *context, void *buffer, int minRead, int maxRead)
{
	ZLibDecompressorState *state = (ZLibDecompressorState *) context;
	z_streamp zp = state->zp;
	int bytesRead = 0;

	while (!IsDecompressionDone(state))
	{
		/* copy remaining data from the output buffer */
		bytesRead += FlushOutputBufferIntoBuffer(state, (char *) buffer, bytesRead,
		                                         maxRead - bytesRead);
		if (bytesRead == maxRead)
		{
			break;
		}

		/* output buffer is fully flushed now */
		zp->avail_out = ZLIB_OUT_SIZE;
		state->outputBufferOffset = 0;

		/* if there are no more bytes in the input buffer, read from source */
		if (zp->avail_in == 0)
		{
			FillInputBufferFromSource(state);
		}

		DecompressInputBufferIntoOutputBuffer(state);
	}

	return bytesRead;
}


/*
 * IsDecompressionDone determines whether decompression is finished, meaning there
 * are no more input bytes to read, and the input buffer and output buffers are empty.
 */
static bool
IsDecompressionDone(ZLibDecompressorState *state)
{
	z_streamp zp = state->zp;

	return state->endOfInputReached &&
	       zp->avail_in == 0 &&
	       zp->avail_out == ZLIB_IN_SIZE;
}


/*
 * FlushOutputBufferIntoBuffer copies bytes from the output buffer into buffer
 * until the output buffer is empty or maxRead bytes were copied.
 */
static int
FlushOutputBufferIntoBuffer(ZLibDecompressorState *state, char *buffer,
                            int bufferOffset, int maxRead)
{
	z_streamp zp = state->zp;

	int numBytesInOutputBuffer = ZLIB_OUT_SIZE - zp->avail_out;
	int bytesRemainingInOutputBuffer = numBytesInOutputBuffer - state->outputBufferOffset;

	if (bytesRemainingInOutputBuffer == 0)
	{
		/* no data to copy */
		return 0;
	}

	/* cannot copy more than what's available or what the caller asked for */
	int bytesCopied = Min(maxRead, bytesRemainingInOutputBuffer);

	memcpy(buffer + bufferOffset, state->outputBuffer + state->outputBufferOffset,
	       bytesCopied);

	state->outputBufferOffset += bytesCopied;

	return bytesCopied;
}


/*
 * FillInputBufferFromSource reads bytes from ByteSource until the inputBuffer
 * is full.
 *
 * The caller must ensure the input buffer is empty before calling the function.
 */
static void
FillInputBufferFromSource(ZLibDecompressorState *state)
{
	z_streamp zp = state->zp;
	ByteSource *byteSource = state->byteSource;

	while (!state->endOfInputReached && zp->avail_in < ZLIB_IN_SIZE)
	{
		int spaceAvailableInInputBuffer = ZLIB_IN_SIZE - zp->avail_in;

		int bytesRead = byteSource->read(byteSource->context,
		                                 state->inputBuffer + zp->avail_in , 0,
		                                 spaceAvailableInInputBuffer);
		if (bytesRead == 0)
		{
			state->endOfInputReached = true;
		}
		else
		{
			/* prepare input buffer */
			zp->next_in = (void *) state->inputBuffer;
			zp->avail_in += bytesRead;
		}

		CHECK_FOR_INTERRUPTS();
	}
}


/*
 * DecompressInputBufferIntoOutputBuffer decompressed bytes from the
 * input buffer until the output buffer is full or the end of the
 * stream is reached.
 *
 * The caller must ensure outputBuffer is empty before calling the function.
 */
static void
DecompressInputBufferIntoOutputBuffer(ZLibDecompressorState *state)
{
	z_streamp zp = state->zp;

	if (zp->avail_in == 0)
	{
		/* nothing to decompress */
		return;
	}

	Assert(zp->avail_out == ZLIB_OUT_SIZE);

	zp->next_out = (void *) state->outputBuffer;

	int resultCode = inflate(zp, 0);
	if (resultCode != Z_OK && resultCode != Z_STREAM_END)
	{
		ereport(ERROR, (errmsg("could not uncompress data: %s", zp->msg)));
	}

	state->outputBuffer[ZLIB_OUT_SIZE - zp->avail_out] = '\0';
}


/*
 * ZLibDecompressorClose closes the compression stream and frees decompressor
 * data structures.
 */
static void
ZLibDecompressorClose(void *context)
{
	ZLibDecompressorState *state = (ZLibDecompressorState *) context;
	ByteSource *byteSource = state->byteSource;
	z_streamp zp = state->zp;

	if (inflateEnd(zp) != Z_OK)
	{
		ereport(ERROR, (errmsg("could not close compression stream: %s", zp->msg)));
	}

	byteSource->close(byteSource->context);

	pfree(state->inputBuffer);
	pfree(state->outputBuffer);
	pfree(state->zp);
	pfree(state);
}

#endif
