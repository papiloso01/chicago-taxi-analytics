"""Bounded-memory Socrata extraction with keyset pagination and retries."""
import json
import re
import time
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from datetime import date


def pages(start, end, dataset="ajtu-isnz", token="", page_size=5000, fetch=None):
    if date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError("start must be earlier than end (exclusive)")
    if not re.fullmatch(r"[a-z0-9]{4}-[a-z0-9]{4}", dataset):
        raise ValueError("invalid dataset ID")
    if not 1 <= page_size <= 50000:
        raise ValueError("page_size must be 1..50000")
    fetch = fetch or request_json
    cursor = None
    while True:
        where = (f"trip_start_timestamp >= '{start}T00:00:00' "
                 f"AND trip_start_timestamp < '{end}T00:00:00'")
        if cursor:
            where += " AND trip_id > '" + cursor.replace("'", "''") + "'"
        params = {"$where": where, "$order": "trip_id ASC", "$limit": page_size}
        url = f"https://data.cityofchicago.org/resource/{dataset}.json?" + urlencode(params)
        rows = fetch(url, token)
        if not rows:
            return
        if any(not row.get("trip_id") for row in rows):
            raise ValueError("API row missing trip_id")
        next_cursor = rows[-1]["trip_id"]
        if cursor is not None and next_cursor <= cursor:
            raise ValueError("API pagination did not advance")
        yield rows
        cursor = next_cursor
        if len(rows) < page_size:
            return


def request_json(url, token):
    headers = {"User-Agent": "taxi-analytics-portfolio/1.0"}
    if token:
        headers["X-App-Token"] = token
    for attempt in range(5):
        try:
            with urlopen(Request(url, headers=headers), timeout=90) as response:
                result = json.load(response)
            if not isinstance(result, list):
                raise ValueError("API returned a non-list response")
            return result
        except (HTTPError, URLError, TimeoutError) as exc:
            if isinstance(exc, HTTPError) and exc.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == 4:
                raise
            time.sleep(min(2 ** attempt, 30))
