/*-------------------------------------------------------------------------
 *
 * set_returning_functions.h
 *	  Utilities for creating set-returning functinos
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef SET_RETURNING_FUNCTIONS_H
#define SET_RETURNING_FUNCTIONS_H


#include "funcapi.h"


ReturnSetInfo * CheckTuplestoreReturn(FunctionCallInfo fcinfo, TupleDesc *tupdesc);
Tuplestorestate * SetupTuplestore(FunctionCallInfo fcinfo, TupleDesc *tupdesc);


#endif
