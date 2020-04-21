/*-------------------------------------------------------------------------
 *
 * copy_utils.h
 *	  Utilities for COPY commands
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef COPY_UTILS_H
#define COPY_UTILS_H


#include "access/tupdesc.h"
#include "nodes/pg_list.h"


List * TupleDescColumnNameList(TupleDesc tupleDescriptor);
List * ColumnNameListToCopyStmtAttributeList(List *columnNameList);


#endif
