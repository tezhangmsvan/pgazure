/*-------------------------------------------------------------------------
 *
 * cpp_utils.h
 *	  Utilities for C++ integration
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef CPP_UTILS_H
#define CPP_UTILS_H
#ifdef __cplusplus
extern "C" {
#endif

bool IsQueryCancelPending(void);
void ThrowPostgresError(const char *message);


#ifdef __cplusplus
}
#endif
#endif
