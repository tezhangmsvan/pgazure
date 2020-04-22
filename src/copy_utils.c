#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_attribute.h"
#include "pgazure/copy_utils.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "utils/lsyscache.h"


/*
 * TupleDescColumnNameList returns a list of column names (char *) for a TupleDesc.
 */
List *
TupleDescColumnNameList(TupleDesc tupleDescriptor)
{
	int columnCount = tupleDescriptor->natts;
	List *columnNameList = NIL;

	/* build the list of column names for remote COPY statements */
	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescriptor, columnIndex);
		char *columnName = NameStr(currentColumn->attname);

		if (currentColumn->attisdropped
#if PG_VERSION_NUM >= 120000
			|| currentColumn->attgenerated == ATTRIBUTE_GENERATED_STORED
#endif
			)
		{
			continue;
		}

		columnNameList = lappend(columnNameList, columnName);
	}

	return columnNameList;
}


/*
 * ColumnNameListToCopyStmtAttributeList converts a list of column names (char *) to
 * an attribute list (Value *) that can be used in a CopyStmt.
 */
List *
ColumnNameListToCopyStmtAttributeList(List *columnNameList)
{
	List *attributeList = NIL;
	ListCell *columnNameCell = NULL;

    /* wrap the column names as Values */
    foreach(columnNameCell, columnNameList)
    {   
        char *columnName = (char *) lfirst(columnNameCell);
        Value *columnNameValue = makeString(columnName);
        
        attributeList = lappend(attributeList, columnNameValue);
    }

	return attributeList;
}


/*
 * TypeOutputFunctions takes an array of types and returns an array of output functions
 * for those types.
 */
FmgrInfo *
TypeOutputFunctions(uint32 columnCount, Oid *typeIdArray, bool binaryFormat)
{
	FmgrInfo *columnOutputFunctions = palloc0(columnCount * sizeof(FmgrInfo));

	for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FmgrInfo *currentOutputFunction = &columnOutputFunctions[columnIndex];
		Oid columnTypeId = typeIdArray[columnIndex];
		bool typeVariableLength = false;
		Oid outputFunctionId = InvalidOid;

		if (columnTypeId == InvalidOid)
		{
			/* TypeArrayFromTupleDescriptor decided to skip this column */
			continue;
		}
		else if (binaryFormat)
		{
			getTypeBinaryOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
		}
		else
		{
			getTypeOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
		}

		fmgr_info(outputFunctionId, currentOutputFunction);
	}

	return columnOutputFunctions;
}


/*
 * ColumnOutputFunctions is a wrapper around TypeOutputFunctions, it takes a
 * tupleDescriptor and returns an array of output functions, one for each column in
 * the tuple.
 */
FmgrInfo *
ColumnOutputFunctions(TupleDesc rowDescriptor, bool binaryFormat)
{
	uint32 columnCount = (uint32) rowDescriptor->natts;
	Oid *columnTypes = TypeArrayFromTupleDescriptor(rowDescriptor);
	FmgrInfo *outputFunctions =
		TypeOutputFunctions(columnCount, columnTypes, binaryFormat);

	return outputFunctions;
}


/*
 * Walks a TupleDesc and returns an array of the types of each attribute.
 * Returns InvalidOid in the place of dropped or generated attributes.
 */
Oid *
TypeArrayFromTupleDescriptor(TupleDesc tupleDescriptor)
{
	int columnCount = tupleDescriptor->natts;
	Oid *typeArray = palloc0(columnCount * sizeof(Oid));

	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDescriptor, columnIndex);
		if (attr->attisdropped
#if PG_VERSION_NUM >= 120000
			|| attr->attgenerated == ATTRIBUTE_GENERATED_STORED
#endif
			)
		{
			typeArray[columnIndex] = InvalidOid;
		}
		else
		{
			typeArray[columnIndex] = attr->atttypid;
		}
	}

	return typeArray;
}



