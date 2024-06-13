## Cosmos PL: Alternative Azure Cosmos library

### What this library addresses that I don't like about MS library.

* The MS library does excessive logging so (in my case), Azure Functions logs have to be turned off or are too costly
https://github.com/Azure/azure-sdk-for-python/issues/12776

* The MS library always converts the raw json into python lists and dicts immediately using built-in base `json`.

* If running FastAPI then there is often no point to parsing cosmos results to python objects only for FastAPI to convert them right back to json.

* Other libraries can parse json faster such as [orjson](https://github.com/ijl/orjson) for python lists and dicts or [polars](https://github.com/pola-rs/polars) to DataFrames. In my experience, polars is better at parsing raw json then row oriented python objects anyway. 

* When you try to read an item that doesn't exist, the MS lib raises an Error so checking if an item exists requires try/except blocks. I'd refer it return None or an empty list.

### Status

This is a WIP. Right now it can return a SQL query as either a polars dataframe, python lists/dicts using orjson, or raw bytes. It uses httpx, rather than requests or aiohttp. It uses async only. Currently, the only method is `query`.

### Quick use example

Create a Cosmos DB instance
```
from cosmospl import Cosmos

cosdb = Cosmos('your_db_name', 'your_container_name', 'your_connection_string')
# if you have an environment variable called 'cosmos' then leave that arg blank.

df = await cosdb.query("select * from c", return_as='pl')
```
### Async

The query functionality is recursive and uses httpx.stream. Sometimes, Cosmos will give results back in pages rather than in a single response. When there will be additional pages then the initial response will have a continuation token as a header. By streaming the result, the header is known before the data is downloaded so then a concurrent request can be made for the 2nd page of data while the first is being downloaded. Since the function is recursive it will request the next page as its downloading the current page. It might be the case that the MS library does this but I'm not sure.

### Warning

On the Cosmos python sdk page it says:

> [WARNING] Using the asynchronous client for concurrent operations like shown in this sample will consume a lot of RUs very fast. We strongly recommend testing this out against the cosmos emulator first to verify your code works well and avoid incurring charges.

Unfortunately, this isn't setup to use their emulator.


### TODO (not necessarily in the order I'll do them):

1. Documentation and docstrings
2. More complete error handling
3. Refactoring (already)
4. Method for creating/upserting records
5. Method for deleting records
6. Method for reading a record with its id/partition_key (rather than via SQL query)
7. Publish to PyPi
8. Optionally, try to convert columns into dates or datetimes when returning to polars

