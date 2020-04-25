/*-------------------------------------------------------------------------
 *
 * storage_account.h
 *	  Utilities for managing storage account credentials.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef STORAGE_ACCOUNT_H
#define STORAGE_ACCOUNT_H


typedef struct StorageAccount
{
	char *accountName;
	char *connectionString;
} StorageAccount;


char * AccountStringToConnectionString(char *accountString);
StorageAccount * GetStorageAccount(const char *accountName);


#endif
