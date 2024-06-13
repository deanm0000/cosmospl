import httpx
import os
import base64
import hmac
import hashlib
import urllib.parse
from datetime import datetime, timezone, timedelta
import orjson
import polars as pl
from typing import TypeAlias, Literal, List, Dict
import asyncio

ALLOWED_RETURNS: TypeAlias = Literal["dict", "pl", "raw"]


class Resp403(Exception):
    pass


class RespFail(Exception):
    pass


def get_inner_content(resp: bytes):
    ## Find "Documents" and ignore outer response
    search_str = 'Documents":['
    begin_range = 0
    range_size = last_range_size = 50
    while True:
        try_find = (
            resp[begin_range : begin_range + range_size].decode("utf8").find(search_str)
        )
        if try_find != -1:
            begin_char = try_find + len(search_str) - 1 + begin_range
            break
        elif begin_range > 200:
            raise ValueError("can't find Documents")
        elif range_size == last_range_size:
            range_size += len(search_str)
        elif range_size > last_range_size:
            begin_range += range_size
            last_range_size = range_size
    search_str = ',"_count"'
    begin_range = -50
    end_range = last_end_range = -1
    while True:
        try_find = (
            resp[begin_range:end_range][::-1].decode("utf8").find(search_str[::-1])
        )
        if try_find != -1:
            end_char = -try_find - len(search_str) + end_range
            break
        else:
            raise ValueError("can't find ending counts")
    return resp[begin_char:end_char]


def gen_sig(
    verb, resource_type, resource_id_or_fullname, x_date, master_key, http_date=""
):
    # decodes the master key which is encoded in base64
    key = base64.b64decode(master_key)

    # Skipping lower casing of resource_id_or_fullname since it may now contain "ID"
    # of the resource as part of the fullname
    text = "{verb}\n{resource_type}\n{resource_id_or_fullname}\n{x_date}\n{http_date}\n".format(
        verb=(verb.lower() or ""),
        resource_type=(resource_type.lower() or ""),
        resource_id_or_fullname=(resource_id_or_fullname or ""),
        x_date=x_date.lower(),
        http_date=http_date.lower(),
    )
    print(text)

    body = text.encode("utf-8")
    digest = hmac.new(key, body, hashlib.sha256).digest()
    signature = base64.encodebytes(digest).decode("utf-8")

    master_token = "master"
    token_version = "1.0"
    secret = "type={type}&ver={ver}&sig={sig}".format(
        type=master_token, ver=token_version, sig=signature[:-1]
    )
    print(secret)
    return secret


class Cosmos:
    def __init__(
        self,
        db: str,
        container: str,
        conn_str: str | None = None,
        default_partition_key: str = None,
    ):
        if conn_str is None and "cosmos" in os.environ:
            conn_str = os.environ["cosmos"]
        self.db = db

        self.container = container
        self.session = None
        self.partition_key = default_partition_key
        self.url_suffix = ""
        account_dict = {
            (y := x.split("=", maxsplit=1))[0]: y[1] for x in conn_str.split(";")
        }
        url = account_dict["AccountEndpoint"]
        while url[-1] == "/":
            url = url[0:-1]
        self.base_url = url
        self.client = httpx.AsyncClient()
        self.key = account_dict["AccountKey"]

    def set_default_partition_key(self, default_partition_key: str = None):
        self.partition_key = default_partition_key

    def make_headers(
        self,
        is_query: bool,
        resource_type: str,
        resource_link: str,
        max_item: int | str = None,
        continuation: str = None,
        partition_key: str = None,
    ):
        date = (datetime.now(tz=timezone.utc) - timedelta(seconds=3)).strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )
        headers = {
            "x-ms-version": "2020-07-15",
            "x-ms-documentdb-isquery": str(is_query).lower(),
            "Content-Type": "application/query+json"
            if is_query
            else "application/json",
            "x-ms-date": date,
            "authorization": gen_sig(
                "post", resource_type, resource_link, date, self.key
            ),
        }
        if max_item is not None:
            headers["x-ms-max-item-count"] = str(max_item)
        if self.session is not None:
            headers["x-ms-session-token"] = self.session
        if continuation is not None:
            headers["x-ms-continuation"] = continuation
        if partition_key is None:
            headers["x-ms-documentdb-query-enablecrosspartition"] = "true"
        else:
            headers["x-ms-documentdb-partitionkey"] = '["' + partition_key + '"]'
            headers["x-ms-documentdb-query-enablecrosspartition"] = "false"
        return headers

    async def query(
        self,
        query: str,
        params: List[Dict[str, str]] = None,
        partition_key: str = None,
        return_as: ALLOWED_RETURNS = "dict",
        max_item:int|str=None
    ):
        if params is None:
            params=[]
        body = {"query": query, "parameters": params}
        if partition_key is None and self.partition_key is not None:
            partition_key = self.partition_key
        resource_type = "docs"
        resource_link = f"dbs/{self.db}/colls/{self.container}"
        # all_results=[]
        # while True:
        headers = self.make_headers(
            True,
            resource_type,
            resource_link,
            partition_key=partition_key,
            max_item=max_item
        )
        # resp = await self.client.post(f"//{resource_link}/docs", json=body, headers=headers)
        resp_bytes = await self.get_stream(
            self.base_url + f"//{resource_link}/docs" + self.url_suffix,
            json=body,
            headers=headers,
        )

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

    async def get_stream(self, url, *, json, headers, continued=0):
        print(f"continued {continued}")
        print(url)
        async with self.client.stream("POST", url, json=json, headers=headers) as sresp:
            print(sresp)
            print(sresp.request.url)
            # if sresp.status_code == 403:
            #     raise Resp403(f"got 403")
            # elif sresp.status_code != 200:
            #     raise RespFail(f"got {sresp.status_code}")
            if "x-ms-session-token" in sresp.headers:
                self.session = sresp.headers.get("x-ms-session-token")
            if "x-ms-continuation" in sresp.headers:
                new_headers = {
                    **headers,
                    **{
                        "x-ms-continuation": sresp.headers.get("x-ms-continuation"),
                        "x-ms-session-token": sresp.headers.get("x-ms-session-token"),
                    },
                }
                next_page = asyncio.create_task(
                    self.get_stream(
                        url, json=json, headers=new_headers, continued=continued + 1
                    )
                )
            else:
                next_page = None
            resp_bytes = []
            async for chunk in sresp.aiter_bytes():
                resp_bytes.append(chunk)
            if sresp.status_code != 200:
                raise RespFail(
                    f"got {sresp.status_code}\n" + b"".join(resp_bytes).decode("utf8")
                )
            if next_page is not None:
                next_page = await next_page
                resp_bytes.append(next_page)
            return b"".join(resp_bytes)
