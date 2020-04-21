#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_attribute.h"
#include "pgazure/copy_utils.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"


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


