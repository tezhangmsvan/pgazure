/*-------------------------------------------------------------------------
 *
 * copy_format_encoder.h
 *      Utilities for encoding a stream of bytes using COPY infrastructure.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef COPY_FORMAT_ENCODER_H
#define COPY_FORMAT_ENCODER_H


#include "postgres.h"
#include "commands/copy.h"
#include "parser/parse_coerce.h"
#include "pgazure/byte_io.h"


/*
 * CitusCopyDest indicates the source or destination of a COPY command.
 */
typedef enum CopyDest
{
	COPY_FILE,                  /* to/from file (or a piped program) */
	COPY_OLD_FE,                /* to/from frontend (2.0 protocol) */
	COPY_NEW_FE,                /* to/from frontend (3.0 protocol) */
	COPY_CALLBACK               /* to/from callback function */
} CopyDest;


/*
 * A smaller version of copy.c's CopyStateData, trimmed to the elements
 * necessary to copy out results. While it'd be a bit nicer to share code,
 * it'd require changing core postgres code.
 */
typedef struct CopyOutStateData
{
	Relation rel;
	CopyDest copy_dest;         /* type of copy source/destination */
	StringInfo fe_msgbuf;       /* used for all dests during COPY TO, only for
                                 * dest == COPY_NEW_FE in COPY FROM */
	List *attnumlist;           /* integer list of attnums to copy */
	int file_encoding;          /* file or remote side's character encoding */
	bool need_transcoding;      /* file encoding diff from server? */
	bool encoding_embeds_ascii; /* ASCII can be non-first byte? */
	bool binary;                /* binary format? */
	bool freeze;                /* freeze rows on loading? */
	bool csv_mode;              /* Comma Separated Value format? */
	bool header_line;           /* CSV header line? */
	char *null_print;           /* NULL marker string (server encoding!) */
	int null_print_len;         /* length of same */
	char *null_print_client;    /* same converted to file encoding */
	char *delim;                /* column delimiter (must be 1 byte) */
	char *quote;                /* CSV quote char (must be 1 byte) */
	char *escape;               /* CSV escape char (must be 1 byte) */
	List *force_quote;          /* list of column names */
	bool force_quote_all;       /* FORCE_QUOTE *? */
	bool *force_quote_flags;    /* per-column CSV FQ flags */
	List *force_notnull;        /* list of column names */
	bool *force_notnull_flags;  /* per-column CSV FNN flags */
	List *force_null;           /* list of column names */
	bool *force_null_flags;     /* per-column CSV FN flags */
	bool convert_selectively;	/* do selective binary conversion? */
	List *convert_select;       /* list of column names (can be NIL) */
	bool *convert_select_flags;	/* per-column CSV/TEXT CS flags */

	MemoryContext rowcontext;   /* per-row evaluation context */
} CopyOutStateData;

typedef struct CopyOutStateData *CopyOutState;

/* struct to allow rReceive to coerce tuples before sending them to workers  */
typedef struct CopyCoercionData
{
	CoercionPathType coercionType;
	FmgrInfo coerceFunction;

	FmgrInfo inputFunction;
	FmgrInfo outputFunction;
	Oid typioparam; /* inputFunction has an extra param */
} CopyCoercionData;

typedef struct CopyFormatEncoder
{
	ByteSink *byteSink;
	TupleDesc tupleDescriptor;
	List *copyOptions;

	CopyOutState copyOutState;
	FmgrInfo *columnOutputFunctions;
} CopyFormatEncoder;



CopyFormatEncoder * CopyFormatEncoderCreate(ByteSink *byteSink,
                                            TupleDesc tupleDescriptor,
                                            List *copyOptions);
void CopyFormatEncoderStart(CopyFormatEncoder *encoder);
void CopyFormatEncoderPush(CopyFormatEncoder *encoder, Datum *columnValues,
                           bool *columnNulls);
void CopyFormatEncoderFinish(CopyFormatEncoder *encoder);


#endif
