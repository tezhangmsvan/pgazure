/*-------------------------------------------------------------------------
 *
 * src/pgazure.c
 *
 * Main entry point for PGAzure.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "pgazure/blob_storage.h"
#include "pgazure/set_returning_functions.h"
#include "utils/builtins.h"
#include "utils/guc.h"


PG_MODULE_MAGIC;


static char *ConnectionString = NULL;


void _PG_init(void);


/*
 * _PG_init gets called when the extension is loaded.
 */
void
_PG_init(void)
{
	DefineCustomStringVariable(
		"azure.connection_string",
		gettext_noop("Database in which pg_cron metadata is kept."),
		NULL,
		&ConnectionString,
		"",
		PGC_USERSET,
		GUC_SUPERUSER_ONLY,
		NULL, NULL, NULL);
}
