/*-------------------------------------------------------------------------
 *
 * cpp_utils.c
 *		Utility functions for interactions with C / PG code from C++
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "pgazure/cpp_utils.h"


bool
IsQueryCancelPending(void)
{
	return (QueryCancelPending || ProcDiePending);
}


void
ThrowPostgresError(const char *message)
{
	ereport(ERROR, (errmsg("%s", message)));
}
