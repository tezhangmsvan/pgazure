#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "pgazure/storage_account.h"
#include "utils/builtins.h"


#define CONNECTION_STRING_PREFIX "DefaultEndpointsProtocol="


/*
 * AccountStringToConnectionString checks whether accountString is a connection
 * string and returns it in that case. Otherwise, it is assumed to be an account
 * name and we look up the storage accounts table.
 */
char *
AccountStringToConnectionString(char *accountString)
{
	if (memcmp(accountString, CONNECTION_STRING_PREFIX,
			   strlen(CONNECTION_STRING_PREFIX)) == 0)
	{
		return accountString;
	}
	else
	{
		return GetStorageAccount(accountString)->connectionString;
	}
}


/*
 * GetAccountTuple returns a StorageAccount struct for the given account
 * name containing the connection string, or NULL if the storage account
 * cannot be found.
 *
 * We use SPI to go through Citus planner hooks.
 */
StorageAccount *
GetStorageAccount(const char *accountName)
{
	Oid argTypes[] = { TEXTOID };
	Datum argValues[] = { CStringGetTextDatum(accountName) };
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;
	bool isNull = false;

	/*
	 * SPI_connect switches to its own memory context, which is destroyed by
	 * the call to SPI_finish. SPI_palloc is provided to allocate memory in
	 * the previous ("upper") context, but that is inadequate when we need to
	 * call other functions that themselves use the normal palloc (such as
	 * lappend). So we switch to the upper context ourselves as needed.
	 */
	MemoryContext upperContext = CurrentMemoryContext, oldContext = NULL;

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT connection_string "
									  "FROM azure.storage_accounts "
									  "WHERE account_name = $1",
									  1, argTypes, argValues, NULL, false, 1);
	Assert(spiStatus == SPI_OK_SELECT);

	if (SPI_processed != 1)
	{
		ereport(ERROR, (errmsg("storage account \"%s\" not found", accountName),
						errhint("Use SELECT azure.add_storage_account('%s', "
		                        "'<connection string>')", accountName)));
	}

	Datum connectionStringDatum = SPI_getbinval(SPI_tuptable->vals[0],
	                                            SPI_tuptable->tupdesc, 1,
	                                            &isNull);

	oldContext = MemoryContextSwitchTo(upperContext);

	StorageAccount *account = (StorageAccount *) palloc0(sizeof(StorageAccount));
	account->accountName = pstrdup(accountName);
	account->connectionString = TextDatumGetCString(connectionStringDatum);

	MemoryContextSwitchTo(oldContext);

	SPI_finish();

	return account;
}
