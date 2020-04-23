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
- `azure.blob_storage_put_blob(connection_string, container_name, path, record)` is an aggregate function for writing a set of records to blob storage.

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

The `blob_storage_get_blob` function retrieves a file from blob storage. In order for `blob_storage_get_blob` to know how to parse the data you can either pass a value with a type that corresponds to the columns in the file, or explicit define the columns in the FROM clause.

```sql
SELECT * FROM azure.blob_storage_get_blob('...','pgazure','customer_reviews_1998.csv', NULL::customer_reviews) LIMIT 3;
┌───────────────┬─────────────┬───────────────┬──────────────┬──────────────────────┬────────────┬────────────────────────────────────────────┬────────────────────┬───────────────
│  customer_id  │ review_date │ review_rating │ review_votes │ review_helpful_votes │ product_id │               product_title                │ product_sales_rank │ product_group
├───────────────┼─────────────┼───────────────┼──────────────┼──────────────────────┼────────────┼────────────────────────────────────────────┼────────────────────┼───────────────
│ AE22YDHSBFYIP │ 1970-12-30  │             5 │           10 │                    0 │ 1551803542 │ Start and Run a Coffee Bar (Start & Run a) │              11611 │ Book         
│ AE22YDHSBFYIP │ 1970-12-30  │             5 │            9 │                    0 │ 1551802538 │ Start and Run a Profitable Coffee Bar      │             689262 │ Book         
│ ATVPDKIKX0DER │ 1995-06-19  │             4 │           19 │                   18 │ 0898624932 │ The Power of Maps                          │             407473 │ Book         
└───────────────┴─────────────┴───────────────┴──────────────┴──────────────────────┴────────────┴────────────────────────────────────────────┴────────────────────┴───────────────
(3 rows)


SELECT * FROM azure.blob_storage_get_blob('...','pgazure','customer_reviews_1998.csv') AS res (
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
  similar_product_ids CHAR(10)[])
 LIMIT 3;                                                                                                                                              
┌───────────────┬─────────────┬───────────────┬──────────────┬──────────────────────┬────────────┬────────────────────────────────────────────┬────────────────────┬───────────────
│  customer_id  │ review_date │ review_rating │ review_votes │ review_helpful_votes │ product_id │               product_title                │ product_sales_rank │ product_group
├───────────────┼─────────────┼───────────────┼──────────────┼──────────────────────┼────────────┼────────────────────────────────────────────┼────────────────────┼───────────────
│ AE22YDHSBFYIP │ 1970-12-30  │             5 │           10 │                    0 │ 1551803542 │ Start and Run a Coffee Bar (Start & Run a) │              11611 │ Book         
│ AE22YDHSBFYIP │ 1970-12-30  │             5 │            9 │                    0 │ 1551802538 │ Start and Run a Profitable Coffee Bar      │             689262 │ Book         
│ ATVPDKIKX0DER │ 1995-06-19  │             4 │           19 │                   18 │ 0898624932 │ The Power of Maps                          │             407473 │ Book         
└───────────────┴─────────────┴───────────────┴──────────────┴──────────────────────┴────────────┴────────────────────────────────────────────┴────────────────────┴───────────────
(3 rows)
```

The `blob_storage_put_blob` aggregate writes a set of records to a file in blob storage..
```sql
SELECT azure.blob_storage_put_blob('...','pgazure','customer_reviews_all.csv.gz', customer_reviews) FROM customer_reviews;
```
