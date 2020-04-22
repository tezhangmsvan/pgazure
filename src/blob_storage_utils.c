#include "postgres.h"

#include "pgazure/blob_storage_utils.h"


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


