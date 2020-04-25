#include "postgres.h"

#include "pgazure/blob_storage_utils.h"


/*
 * CodecStringFromFileName tries to guess the encoder/decoder string
 * from the suffix of a file name.
 */
char *
CodecStringFromFileName(char *path)
{
	if (HasSuffix(path, ".csv") || HasSuffix(path, ".csv.gz"))
	{
		return "csv";
	}
	else if (HasSuffix(path, ".tsv") || HasSuffix(path, ".tsv.gz"))
	{
		return "tsv";
	}
	else if (HasSuffix(path, ".json") || HasSuffix(path, ".json.gz"))
	{
		return  "json";
	}
	else if (HasSuffix(path, ".xml") || HasSuffix(path, ".xml.gz"))
	{
		return  "xml";
	}
	else
	{
		return "csv";
	}
}


/*
 * HasSuffix determines whether a filename ends in the given suffix.
 */
bool
HasSuffix(const char *filename, const char *suffix)
{
	int filenamelen = strlen(filename);
	int suffixlen = strlen(suffix);

	if (filenamelen < suffixlen)
	{
		return 0;
	}

	return memcmp(&filename[filenamelen - suffixlen], suffix, suffixlen) == 0;
}


