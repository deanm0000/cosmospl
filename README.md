## Cosmos PL: Alternative Azure Cosmos library

### What this library addresses that I don't like about MS library.

* The MS library does excessive logging so (in my case), Azure Functions logs have to be turned off or are too costly
https://github.com/Azure/azure-sdk-for-python/issues/12776

* The MS library always converts the raw json into python lists and dicts immediately using built-in base `json`.

* If running FastAPI then there is often no point to parsing cosmos results to python objects only for FastAPI to convert them right back to json.

* Other libraries can parse json faster such as [orjson](https://github.com/ijl/orjson) for python lists and dicts or [polars](https://github.com/pola-rs/polars) to DataFrames. In my experience, polars is better at parsing raw json then row oriented python objects anyway. 

* When you try to read an item that doesn't exist, the MS lib raises an Error so checking if an item exists requires try/except blocks. I'd refer it return None or an empty list.

### Quick use example

Create a Cosmos DB instance
```
from cosmospl import Cosmos

cosdb = Cosmos('your_db_name', 'your_container_name', 'your_connection_string')
# if you have an environment variable called 'cosmos' then leave that arg blank.

df = await cosdb.query("select * from c", return_as='pl')
```
### Usage overview and differences from MS

All functionality is inside the `Cosmos` class which is similar to the container client in the MS SDK.

For details please refer to the source.

To initialize the class pass a database name, container name, and (optionally) the connection string to `Cosmos` as ordered arguments. If the connection string is omitted it'll use the `cosmos` environment variable.

The methods in that class are:

`query`: execute a query against the container. Use the `return_as` parameter to specify `pl` for polars dataframe, `dict` for dict/list, `resp` for the httpx response. Unlike MS, it returns everything in one call, it isn't an Async generator.

`query_stream`: executes a query against the container. It returns an async generator of raw json. It is intended to be used in FastAPI streaming responses so it doesn't have to parse json or accumulate results before sending to end-user.

In the case of both query methods, Cosmos returns a nested json where the data is inside a Documents key. In order to avoid parsing this in its entirety while only returning data, it looks for `Documents":[` and then only returns from there. Similarly at the end it truncates from  `,"_count"`.

`create`: creates (not upserts) a record

`upsert`: upserts a record

`delete`: deletes a record

`read`: will read one record based on input id and partition_key

`get_container_meta`: returns meta data about the container

`get_pk_ranges`: returns the pk ranges of the container. Can be useful for doing cross partition query requests in chunks using the `pk_id` parameter

### Warning

On the Cosmos python sdk page it says:

> [WARNING] Using the asynchronous client for concurrent operations like shown in this sample will consume a lot of RUs very fast. We strongly recommend testing this out against the cosmos emulator first to verify your code works well and avoid incurring charges.

### Future polars enhancements (maybe)

1. (optionally) detect datetime columns and automatically convert to pl.Date or pl.Datetime
2. save a df to cosmos directly (possibly) with metadata so columns can be restored to same types when loaded

### Future General Enhancements (maybe)

1. Add top level and database classes