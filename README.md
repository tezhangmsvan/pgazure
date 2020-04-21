# PGAzure

PGAzure is an extension for interacting with Azure services from PostgreSQL. Currently, it implements a set of UDFs for interacting with Azure blob storage.

## Installation

Prerequisites:
- [Microsoft C++ Rest SDK](https://github.com/microsoft/cpprestsdk)
- [Microsoft Azure Storage Client Library for C++](https://github.com/Azure/azure-storage-cpp)

Once you have installed the prerequisites, you can compile and install PGAzure with:
```bash
# Make sure pg_config is in your path, e.g.
export PATH=/usr/pgsql-12/bin:$PATH

make
make install
```

After installing the extension, you can create the extension from `psql` by running:
```sql
CREATE EXTENSION pgazure;
```

## Blob Storage UDFs

PGAzure providers 3 UDFs for interacting with blob storage:

- `azure.blob_storage_list_blobs(connection_string, container_name, prefix)` 
- `azure.blob_storage_get_blob(connection_string, container_name, path)` 
- `azure.blob_storage_put_blob(connection_string, container_name, path, record)` is an aggregate function for writing a set of records to blob storage (currently in CSV format) 

The `blob_storage_list_blobs` lists objects in blob storage:
```sql
SELECT * FROM azure.blob_storage_list_blobs('...','pgazure','customer_reviews');  
┌──────────────────────────────┬───────────┬────────────────────────┬─────────────────────┬──────────────────────────┬──────────────────┬──────────────────┬───────────────────────
│             path             │   size    │     last_modified      │        etag         │       content_type       │ content_encoding │ content_language │       content_md5    
├──────────────────────────────┼───────────┼────────────────────────┼─────────────────────┼──────────────────────────┼──────────────────┼──────────────────┼───────────────────────
│ customer_reviews_1998.csv    │ 101299118 │ 2020-04-19 21:16:35+02 │ "0x8D7E4962D9F0CC7" │ text/csv                 │                  │                  │                      
└──────────────────────────────┴───────────┴────────────────────────┴─────────────────────┴──────────────────────────┴──────────────────┴──────────────────┴───────────────────────
(1 row)
```

The `blob_storage_get_blob` function retrieves a file from blob storage (currently only uncompressed CSV). You need to specify the columns as they appear in the file when querying in order for `blob_storage_get_blob` to correctly parse the data.

```sql
SELECT count(*) FROM azure.blob_storage_get_blob('...','pgazure','customer_reviews_copy.csv') AS res (
  customer_id TEXT,
  review_date DATE,
  review_rating INTEGER,
  review_votes INTEGER,
  review_helpful_votes INTEGER,
  product_id CHAR(10),
  product_title TEXT,
  product_sales_rank BIGINT,
  product_group TEXT,
  product_category TEXT,
  product_subcategory TEXT,
  similar_product_ids CHAR(10)[]);
```

The `blob_storage_put_blob` aggregate writes a set of records to a file in blob storage (currently only uncompressed CSV).
```sql
SELECT azure.blob_storage_put_blob('...','pgazure','my_table.csv', my_table) FROM my_table;
```
