#include "postgres.h"

#include "pgazure/byte_io.h"
#include "pgazure/compression.h"
#include "pgazure/zlib_compression.h"


static CompressionType CompressionTypeFromString(char *string);


/*
 * BuildCompressor builds a compressor from a string.
 */
ByteSink *
BuildCompressor(char *compressorString, ByteSink *byteSink)
{
	ByteSink *compressor = NULL;
	CompressionType compressionType = CompressionTypeFromString(compressorString);

	switch (compressionType)
	{
#ifdef HAVE_LIBZ
		case COMPRESSION_GZIP:
		{
			compressor = CreateZLibCompressor(byteSink);
			break;
		}
#endif

		case COMPRESSION_NONE:
		default:
		{
			compressor = byteSink;
			break;
		}
	}

	return compressor;
}


/*
 * BuildCompressor builds a decompressor from a string.
 */
ByteSource *
BuildDecompressor(char *decompressorString, ByteSource *byteSource)
{
	ByteSource *decompressor = NULL;
	CompressionType compressionType = CompressionTypeFromString(decompressorString);

	switch (compressionType)
	{
#ifdef HAVE_LIBZ
		case COMPRESSION_GZIP:
		{
			decompressor = CreateZLibDecompressor(byteSource);
			break;
		}
#endif

		case COMPRESSION_NONE:
		default:
		{
			decompressor = byteSource;
			break;
		}
	}

	return decompressor;
}


/*
 * CompressionTypeFromString determines the compression type in an compressor/decompressor
 * string.
 */
static CompressionType
CompressionTypeFromString(char *compressionString)
{
	if (strcmp(compressionString, "gzip") == 0)
	{
#ifdef HAVE_LIBZ
		return COMPRESSION_GZIP;
#else
		ereport(ERROR, (errmsg("gzip compression requires postgres to be "
							   "built with zlib")));
#endif
	}
	else if (strcmp(compressionString, "none") == 0)
	{
		return COMPRESSION_NONE;
	}
	else
	{
		ereport(ERROR, (errmsg("invalid compression algorithm: %s", compressionString)));
	}
}
