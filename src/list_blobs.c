/*-------------------------------------------------------------------------
 *
 * list_blobs.c
 *     Implementation the blob_storage_list_blobs UDF
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "pgazure/blob_storage.h"
#include "pgazure/set_returning_functions.h"
#include "pgazure/storage_account.h"
#include "storage/itemptr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/tuplestore.h"


#define LIST_RESULT_NAME_INDEX 0
#define LIST_RESULT_SIZE_INDEX 1
#define LIST_RESULT_LAST_MODIFIED_INDEX 2
#define LIST_RESULT_ETAG_INDEX 3
#define LIST_RESULT_CONTENT_TYPE_INDEX 4
#define LIST_RESULT_CONTENT_ENCODING_INDEX 5
#define LIST_RESULT_CONTENT_LANGUAGE_INDEX 6
#define LIST_RESULT_CONTENT_MD5_INDEX 7


/* avoid repetition when setting fields of a tuple */
#define SetColumnValue(index,value) \
{ \
	columnValues[index] = value; \
	columnNulls[index] = false; \
}

#define SetTextColumnValue(index,value) \
{ \
	if (value != NULL) \
	{ \
		SetColumnValue(index, CStringGetTextDatum(value)); \
	} \
}



PG_FUNCTION_INFO_V1(blob_storage_list_blobs);


/*
 * ListBlobsReceiver contains state that is passed to AddBlobToTupleStore via C++ code.
 */
typedef struct ListBlobsReceiver
{
	Tuplestorestate *tupleStore;
	TupleDesc tupleDescriptor;

	int columnCount;
    Datum *columnValues;
    bool *columnNulls;
} ListBlobsReceiver;


static void AddBlobToTupleStore(void *context, CloudBlob *blob);


/*
 * blob_storage_list_blobs
 */
Datum
blob_storage_list_blobs(PG_FUNCTION_ARGS)
{
	char *accountString = text_to_cstring(PG_GETARG_TEXT_P(0));
	char *containerName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char *prefix = text_to_cstring(PG_GETARG_TEXT_P(2));
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
    int columnCount = tupleDescriptor->natts;

	char *connectionString = AccountStringToConnectionString(accountString);

	struct ListBlobsReceiver receiver;
	receiver.tupleStore = tupleStore;
	receiver.tupleDescriptor = tupleDescriptor;
    receiver.columnValues = palloc0(columnCount * sizeof(Datum));
    receiver.columnNulls = palloc0(columnCount * sizeof(bool));

	ListBlobs(connectionString, containerName, prefix, AddBlobToTupleStore, &receiver);

	PG_RETURN_DATUM(0);
}


/*
 * AddBlobToTupleStore writes the CloudBlob as a tuple to the tuplestore in the
 * ListBlobsReceiver (passed as context).
 */
static void
AddBlobToTupleStore(void *context, CloudBlob *cloudBlob)
{
	struct ListBlobsReceiver *receiver = (struct ListBlobsReceiver *) context;
	Tuplestorestate *tupleStore = receiver->tupleStore;
	TupleDesc tupleDescriptor = receiver->tupleDescriptor;
	Datum *columnValues = receiver->columnValues;
	bool *columnNulls = receiver->columnNulls;

	/* all fields are NULL unless set */
	memset(columnNulls, 1, tupleDescriptor->natts);

	SetTextColumnValue(LIST_RESULT_NAME_INDEX, cloudBlob->name);
	SetColumnValue(LIST_RESULT_SIZE_INDEX, UInt64GetDatum(cloudBlob->size));

	if (cloudBlob->lastModified != NULL)
	{
		Datum lastModifiedDatum = DirectFunctionCall3(timestamptz_in,
	    	                                          CStringGetDatum(cloudBlob->lastModified),
	        	                                      ObjectIdGetDatum(InvalidOid),
	            	                                  Int32GetDatum(-1));

		SetColumnValue(LIST_RESULT_LAST_MODIFIED_INDEX, lastModifiedDatum);
	}

	SetTextColumnValue(LIST_RESULT_ETAG_INDEX, cloudBlob->etag);
	SetTextColumnValue(LIST_RESULT_CONTENT_TYPE_INDEX, cloudBlob->contentType);
	SetTextColumnValue(LIST_RESULT_CONTENT_ENCODING_INDEX, cloudBlob->contentEncoding);
	SetTextColumnValue(LIST_RESULT_CONTENT_LANGUAGE_INDEX, cloudBlob->contentLanguage);
	SetTextColumnValue(LIST_RESULT_CONTENT_MD5_INDEX, cloudBlob->contentMD5);

	tuplestore_putvalues(tupleStore, tupleDescriptor, columnValues, columnNulls);
}
