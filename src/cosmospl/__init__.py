from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import hmac
import os
import warnings
from datetime import datetime, timezone  # timedelta
from typing import Literal, TypeAlias
from urllib.parse import quote

import httpx
import orjson

from cosmospl.exceptions import (
    MustSpecifyPartitionKey,
    NoDocuments,
    Resp401,
    RespFail,
    UnsupportedPartitionKey,
)

with contextlib.suppress(ModuleNotFoundError):
    import polars as pl
has_nest = False
with contextlib.suppress(ModuleNotFoundError):
    import nest_asyncio

    has_nest = True

if has_nest is True:
    nest_asyncio.apply()
ALLOWED_RETURNS: TypeAlias = Literal["dict", "pl", "raw", "pljson", "resp"]
DOC_STR = 'Documents":['
COUNT_STR = ',"_count"'
RESOURCE_TYPES: TypeAlias = Literal[
    "dbs", "colls", "sprocs", "udfs", "triggers", "users", "permissions", "docs"
]


class CosAuth(httpx.Auth):  # noqa: D101
    def __init__(self, master_key):
        self.master_key = master_key

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> asyncio.Generator[httpx.Request, httpx.Response, None]:
        """
        Make auth_flow for httpx Auth.

        Args:
            request (httpx.Request): _description_

        Returns
        -------
            asyncio.Generator[httpx.Request, httpx.Response, None]: _description_
        """
        verb = request.method.lower()
        resource_type = request.headers.get("resource_type")
        request.headers.pop("resource_type")

        resource_id = request.url.path
        while resource_id[0] == "/":
            resource_id = resource_id[1:]
        resource_id_split = resource_id.split("/")
        if resource_id_split[-1] == "docs" or resource_id_split[-1] == "pkranges":
            resource_id = "/".join(resource_id_split[:-1])
        working_dt = datetime.now(tz=timezone.utc)
        # while True:
        x_date = working_dt.strftime("%a, %d %b %Y %H:%M:%S GMT").lower()
        auth = _gen_sig(verb, resource_type, resource_id, x_date, self.master_key)
        # if "+" in auth:
        #     working_dt = working_dt - timedelta(seconds=1)
        # else:
        #     break
        request.headers["x-ms-date"] = x_date
        request.headers["authorization"] = auth
        yield request

    def auth_flow(self, request: httpx.Request):
        """
        Make auth_flow for httpx Auth.

        Args:
            request (httpx.Request): _description_

        Returns
        -------
            asyncio.Generator[httpx.Request, httpx.Response, None]: _description_
        """
        verb = request.method.lower()
        resource_type = request.headers.get("resource_type")
        request.headers.pop("resource_type")

        resource_id = request.url.path
        while resource_id[0] == "/":
            resource_id = resource_id[1:]
        resource_id_split = resource_id.split("/")
        if resource_id_split[-1] == "docs" or resource_id_split[-1] == "pkranges":
            resource_id = "/".join(resource_id_split[:-1])
        working_dt = datetime.now(tz=timezone.utc)
        # while True:
        x_date = working_dt.strftime("%a, %d %b %Y %H:%M:%S GMT").lower()
        auth = _gen_sig(verb, resource_type, resource_id, x_date, self.master_key)
        # if "+" in auth:
        #     working_dt = working_dt - timedelta(seconds=1)
        # else:
        #     break
        request.headers["x-ms-date"] = x_date
        request.headers["authorization"] = auth
        yield request


def get_inner_content(resp: bytes, check_docs=True, check_count=True) -> bytes:
    """
    Extract Documents from json without fully parsing.

    The Cosmos responses is json with the data inside a Documents key and other
    superfluous meta data. This function takes out the Documents section
    without fully parsing the json.

    Args:
        resp (bytes): The response contents after concat

    Returns
    -------
    bytes: The Documents only response as a list
    """
    if check_docs is True:
        begin_range = 0
        range_size = last_range_size = 50
        while True:
            try_find = (
                resp[begin_range : begin_range + range_size]
                .decode("utf8")
                .find(DOC_STR)
            )
            if try_find != -1:
                begin_char = try_find + len(DOC_STR) - 1 + begin_range
                break
            elif begin_range > 200:
                msg = "can't find Documents"
                raise NoDocuments(msg)
            elif range_size == last_range_size:
                range_size += len(DOC_STR)
            elif range_size > last_range_size:
                begin_range += range_size
                last_range_size = range_size
    else:
        begin_char = 0

    if check_count is True:
        begin_range = -50
        # TODO: finish the backward checking similar to forward checking
        end_range = last_end_range = -1  # noqa: F841
        while True:
            try_find = (
                resp[begin_range:end_range][::-1].decode("utf8").find(COUNT_STR[::-1])
            )
            if try_find != -1:
                end_char = -try_find - len(COUNT_STR) + end_range
                break
            else:
                msg = "can't find ending counts"
                raise ValueError(msg)
    else:
        end_char = None

    return resp[begin_char:end_char]


def _gen_sig(
    verb: str,
    resource_type: str,
    resource_id_or_fullname: str,
    x_date: str,
    master_key: str,
    http_date: str = "",
):
    key = base64.b64decode(master_key)

    verb = verb.lower() or ""
    resource_type = resource_type.lower() or ""
    resource_id_or_fullname = resource_id_or_fullname or ""
    x_date = x_date.lower()
    http_date = http_date.lower()

    text = (
        f"{verb}\n{resource_type}\n{resource_id_or_fullname}\n{x_date}\n{http_date}\n"
    )

    body = text.encode("utf-8")
    digest = hmac.new(key, body, hashlib.sha256).digest()
    signature = base64.encodebytes(digest).decode("utf-8")

    master_token = "master"
    token_version = "1.0"
    # print(text)
    secret = f"type={master_token}&ver={token_version}&sig={signature[:-1]}"
    # print(secret)
    return quote(secret, "-_.!~*'()")


class Cosmos:
    """Class for interacting with Cosmos container."""

    def __init__(
        self,
        db: str,
        container: str,
        conn_str: str | None = None,
        return_as: ALLOWED_RETURNS = "dict",
        default_partition_key: str | None = None,
    ):
        if conn_str is None and "cosmos" in os.environ:
            conn_str = os.environ["cosmos"]  # noqa: SIM112
        self.db = db

        self.container = container
        self.session = None
        self.partition_key = default_partition_key

        account_dict = {
            (y := x.split("=", maxsplit=1))[0]: y[1] for x in conn_str.split(";")
        }
        url = account_dict["AccountEndpoint"]
        while url[-1] == "/":
            url = url[0:-1]
        self.base_url = url
        self.client = httpx.AsyncClient(auth=CosAuth(account_dict["AccountKey"]))

        self.return_as = return_as
        if has_nest is True:
            loop = asyncio.get_event_loop()
            # Schedule the coroutine and get the result
            future = asyncio.ensure_future(self.get_container_meta(return_as="dict"))
            meta = loop.run_until_complete(future)
        else:
            meta = self._get_container_meta_sync()
        self.meta = meta
        if (
            "partitionKey" in meta
            and "paths" in meta["partitionKey"]
            and len(meta["partitionKey"]["paths"]) == 1
        ):
            part_name = meta["partitionKey"]["paths"][0]
            while part_name[0] == "/":
                part_name = part_name[1:]
            self.partition_key_name = part_name
        else:
            print(meta)
            warnings.warn(UnsupportedPartitionKey, stacklevel=2)

    def set_default_partition_key(self, default_partition_key: str | None = None):
        """Change default partition key to be used in queries."""
        self.partition_key = default_partition_key

    def _make_headers(
        self,
        *,
        is_query: bool | None = None,
        is_upsert: bool | None = None,
        resource_type: RESOURCE_TYPES = "docs",
        max_item: int | str | None = None,
        continuation: str | None = None,
        partition_key: str | None = None,
        pk_id: str | int | None = None,
    ):
        headers = {
            "x-ms-version": "2020-07-15",
            "resource_type": resource_type,
            "user-agent": "python-cosmospl",
        }
        # The resource_type header is for the auth class and is popped before sending
        if pk_id is not None:
            headers["x-ms-documentdb-partitionkeyrangeid"] = str(pk_id)
        if is_upsert is not None:
            headers["x-ms-documentdb-is-upsert"] = str(is_upsert).lower()
        if partition_key is None:
            partition_key = self.partition_key
        if partition_key is not None:
            headers["x-ms-documentdb-partitionkey"] = '["' + partition_key + '"]'
        if is_query is not None:
            headers["x-ms-documentdb-isquery"] = str(is_query).lower()
            if is_query is True:
                headers["Content-Type"] = "application/query+json"
                if partition_key is None:
                    headers["x-ms-documentdb-query-enablecrosspartition"] = "true"
                else:
                    headers["x-ms-documentdb-query-enablecrosspartition"] = "false"
        if max_item is not None:
            headers["x-ms-max-item-count"] = str(max_item)
        if self.session is not None:
            headers["x-ms-session-token"] = self.session
        if continuation is not None:
            headers["x-ms-continuation"] = continuation

        return headers

    async def query(
        self,
        query: str,
        params: list[dict[str, str]] | None = None,
        partition_key: str | None = None,
        return_as: ALLOWED_RETURNS | None = None,
        max_item: int | str | None = None,
        max_retries: int = 5,
        pk_id: str | int | None = None,
    ):
        """
        Perform query and return all results.

        Args:
            query (str): SQL query
            params (List[Dict[str, str]], optional): Params for query or None.
            partition_key (str, optional): The partition key. If none then cross
            partition is enabled.
            return_as: The return type either dict, pl, raw, resp
            max_item (int | str, optional): Max items per request.
            max_retries: The max_retries for Auth

        Returns
        -------
            _type_: _description_
        """
        retries = 0
        if return_as is None:
            return_as = self.return_as
        return await self._query(
            query,
            params,
            partition_key,
            return_as,
            max_item,
            retries,
            max_retries,
            pk_id,
        )

    async def _query(
        self,
        query: str,
        params: list[dict[str, str]] | None = None,
        partition_key: str | None = None,
        return_as: ALLOWED_RETURNS | None = None,
        max_item: int | str | None = None,
        retries: int = 0,
        max_retries: int = 5,
        pk_id: str | int | None = None,
    ):
        """
        Private query meant for recursion with retries.

        Args:
            query (str): SQL query
            params (List[Dict[str, str]], optional): Params for query or None.
            partition_key (str, optional): The partition key. If none then cross
            partition is enabled.
            return_as (ALLOWED_RETURNS, optional): The return type either dict, pl, raw
            max_item (int | str, optional): _description_. Defaults to None.

        Returns
        -------
            _type_: _description_
        """
        if return_as is None:
            return_as = self.return_as
        params, body, headers, url = self._prep_query(
            query,
            params,
            partition_key,
            max_item,
            pk_id,
        )

        if return_as == "resp":
            resp = await self._get_resp(
                url,
                json=body,
                headers=headers,
            )
            if "x-ms-session-token" in resp.headers:
                self.session = resp.headers["x-ms-session-token"]
            return resp
        try:
            resp_bytes = await self._get_stream(
                url,
                json=body,
                headers=headers,
            )
        except Resp401:
            await self.client.aclose()
            if retries < max_retries:
                print("got 401, retrying")
                self.client = httpx.AsyncClient()
                return self._query(
                    query,
                    params,
                    partition_key,
                    return_as,
                    max_item,
                    retries=retries + 1,
                    max_retries=max_retries,
                )
            else:
                raise
        if return_as == "dict":
            return orjson.loads(resp_bytes)["Documents"]
        elif return_as == "pljson":
            return (
                pl.read_json(resp_bytes)
                .select(pl.col("Documents").explode())
                .unnest("Documents")
            )
        elif return_as == "pl":
            return pl.read_json(get_inner_content(resp_bytes))
        else:
            return resp_bytes

    def _prep_query(
        self,
        query: str,
        params: list[dict[str, str]] | None = None,
        partition_key: str | None = None,
        max_item: int | str | None = None,
        pk_id: int | str | None = None,
    ):
        if params is None:
            params = []
        body = {"query": query, "parameters": params}
        # resource_link = f"dbs/{self.db}/colls/{self.container}"

        headers = self._make_headers(
            is_query=True,
            resource_type="docs",
            max_item=max_item,
            partition_key=partition_key,
            pk_id=pk_id,
        )

        url = self.base_url + f"//dbs/{self.db}/colls/{self.container}/docs"
        return (params, body, headers, url)

    async def query_stream(
        self,
        query: str,
        params: list[dict[str, str]] | None = None,
        partition_key: str | None = None,
        max_item: int | str | None = None,
    ):
        """
        Perform query and return all results as a generator.

        Args:
            query (str): SQL query
            params (List[Dict[str, str]], optional): Params for query or None.
            partition_key (str, optional): The partition key. If none then cross
            partition is enabled.
            return_as: The return type either dict, pl, raw, resp
            max_item (int | str, optional): Max items per request.
            max_retries: The max_retries for Auth

        Returns
        -------
            bytes: Generator response
        """
        params, body, headers, url = self._prep_query(
            query,
            params,
            partition_key,
            max_item,
        )
        first_stream = True
        while True:
            first_chunk = True
            prev_chunk = None
            async with self.client.stream(
                "POST", url, json=body, headers=headers
            ) as resp:
                if resp.status_code != 200:
                    msg = f"Status code = {resp.status_code}"
                    raise RespFail(msg)
                if "x-ms-session-token" in resp.headers:
                    self.session = resp.headers.get("x-ms-session-token")
                if "x-ms-continuation" in resp.headers:
                    headers = {
                        **headers,
                        "x-ms-continuation": resp.headers.get("x-ms-continuation"),
                        "x-ms-session-token": resp.headers.get("x-ms-session-token"),
                    }
                    last_stream = False
                else:
                    last_stream = True
                async for chunk in resp.aiter_bytes():
                    if first_chunk is True and first_stream is True:
                        prev_chunk = get_inner_content(chunk, first_chunk, False)
                        first_chunk = False
                    elif first_chunk is True and first_stream is False:
                        prev_chunk = get_inner_content(chunk, first_chunk, False)[1:]
                        first_chunk = False
                    else:
                        yield prev_chunk
                        await asyncio.sleep(0)
                        prev_chunk = chunk
                first_stream = False
                if last_stream is True:
                    yield get_inner_content(prev_chunk, False, True)
                    await asyncio.sleep(0)
                    break
                else:
                    yield get_inner_content(prev_chunk, False, True)[:-1] + b","
                    await asyncio.sleep(0)

    async def _get_resp(self, url, *, json, headers):
        resp = await self.client.post(url, json=json, headers=headers)
        if resp.status_code == 401:
            msg = resp.text
            raise Resp401(msg)
        elif resp.status_code != 200:
            msg = resp.text
            raise RespFail(msg)
        return resp

    async def _get_stream(self, url, *, json, headers, continued=0):
        async with self.client.stream("POST", url, json=json, headers=headers) as resp:
            if "x-ms-session-token" in resp.headers:
                self.session = resp.headers.get("x-ms-session-token")
            if "x-ms-continuation" in resp.headers:
                new_headers = {
                    **headers,
                    "x-ms-continuation": resp.headers.get("x-ms-continuation"),
                    "x-ms-session-token": resp.headers.get("x-ms-session-token"),
                }
                next_page = asyncio.create_task(
                    self._get_stream(
                        url, json=json, headers=new_headers, continued=continued + 1
                    )
                )
            else:
                next_page = None
            if "x-ms-session-token" in resp.headers:
                self.session = resp.headers["x-ms-session-token"]
            resp_bytes = []
            async for chunk in resp.aiter_bytes():
                resp_bytes.append(chunk)
            if resp.status_code == 401:
                msg = b"".join(resp_bytes).decode("utf8")
                raise Resp401(msg)
            elif resp.status_code != 200:
                msg = f"got {resp.status_code}\n" + b"".join(resp_bytes).decode("utf8")
                raise RespFail(msg)
            if next_page is not None:
                next_page = await next_page
                resp_bytes.append(next_page)
            return b"".join(resp_bytes)

    async def _create_or_upsert(self, record, is_upsert=False):
        url = self.base_url + f"//dbs/{self.db}/colls/{self.container}/docs"
        if self.partition_key_name in record:
            partition_key = record[self.partition_key_name]
        elif self.partition_key is not None:
            partition_key = self.partition_key
        else:
            raise MustSpecifyPartitionKey
        headers = self._make_headers(
            resource_type="docs", is_upsert=is_upsert, partition_key=partition_key
        )
        resp = await self.client.post(url, json=record, headers=headers)
        if "x-ms-session-token" in resp.headers:
            self.session = resp.headers["x-ms-session-token"]
        return resp

    async def create(self, record: dict | list):
        """
        Creates a record in the cosmos container.

        Args:
            record dict | list: The record to add

        Returns
        -------
            _type_: _description_
        """
        return await self._create_or_upsert(record, is_upsert=False)

    async def upsert(self, record: dict | list):
        """
        Upserts a record in the cosmos container.

        Args:
            record dict: The record to add

        Returns
        -------
            _type_: _description_
        """
        return await self._create_or_upsert(record, is_upsert=True)

    async def delete(self, id: str, partition_key: str | None = None):
        """
        Delete a record in the cosmos container.

        Args:
            id (str): The id to be deleted
            partition_key (str): The partition from which the id comes
        """
        headers = self._make_headers(partition_key=partition_key)

        url = f"{self.base_url}/dbs/{self.db}/colls/{self.container}/docs/{id}"

        resp = await self.client.delete(url, headers=headers)
        if "x-ms-session-token" in resp.headers:
            self.session = resp.headers["x-ms-session-token"]
        if resp.is_success:
            return "Deleted"
        else:
            return f"Didn't delete \n{resp.text}"

    async def read(
        self,
        id: str,
        partition_key: str | None = None,
        return_as: ALLOWED_RETURNS | None = None,
        max_retries: int = 5,
    ):
        """
        Read a record in the cosmos container.

        Args:
            id (str): The id to be read
            partition_key (str): The partition from which the id comes
            return_as: The return type either dict, pl, raw, resp
        """
        if return_as is None:
            return_as = self.return_as
        resp = await self._read(id, partition_key, retries=0, max_retries=max_retries)
        return self._apply_return_as(resp, return_as)

    async def _read(
        self,
        id: str,
        partition_key: str | None = None,
        retries: int = 0,
        max_retries: int = 5,
    ):
        resource_type = "docs"
        headers = self._make_headers(
            resource_type=resource_type, partition_key=partition_key
        )

        url = f"{self.base_url}/dbs/{self.db}/colls/{self.container}/docs/{id}"

        resp = await self.client.get(url, headers=headers)
        if resp.status_code == 401 and retries < max_retries:
            resp = await self._read(
                id, partition_key, retries=retries + 1, max_retries=max_retries
            )
        elif not resp.is_success:
            msg = resp.text
            raise RespFail(msg)
        return resp

    def _apply_return_as(self, resp: httpx.Response, return_as: ALLOWED_RETURNS):
        if return_as == "resp":
            return resp
        elif return_as == "dict":
            return orjson.loads(resp.content)
        elif return_as in ["pljson", "pl"]:
            return pl.read_json(resp.content)

    async def get_container_meta(self, return_as: ALLOWED_RETURNS | None = None):
        """
        Get Container meta data.

        Args:
            return_as (str, optional): _description_.

        Returns
        -------
            _type_: _description_
        """
        if return_as is None:
            return_as = self.return_as
        url = f"{self.base_url}/dbs/{self.db}/colls/{self.container}"
        headers = self._make_headers(resource_type="colls")
        resp = await self.client.get(url, headers=headers)
        return self._apply_return_as(resp, return_as)

    def _get_container_meta_sync(self, return_as: ALLOWED_RETURNS | None = None):
        if return_as is None:
            return_as = self.return_as
        sync_client = httpx.Client(auth=CosAuth(self.client.auth.master_key))
        url = f"{self.base_url}/dbs/{self.db}/colls/{self.container}"
        headers = self._make_headers(resource_type="colls")
        resp = sync_client.get(url, headers=headers)
        return self._apply_return_as(resp, return_as)

    async def get_pk_ranges(self, return_as: ALLOWED_RETURNS | None = None):
        """
        Get Container pk ranges.

        Returns
        -------
            _type_: _description_
        """
        if return_as is None:
            return_as = self.return_as
        url = f"{self.base_url}/dbs/{self.db}/colls/{self.container}/pkranges"
        headers = self._make_headers(resource_type="pkranges")
        resp = await self.client.get(url, headers=headers)
        return self._apply_return_as(resp, return_as)
