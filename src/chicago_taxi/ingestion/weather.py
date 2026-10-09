"""Versioned geography and historical weather ingestion; all dates end-exclusive."""
import argparse
import json
import math
import os
import time
import uuid
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError

ZONE=ZoneInfo("America/Chicago")
VARIABLES=["temperature_2m","precipitation","snowfall"]
def get_json(url):
    for attempt in range(5):
        try:
            with urlopen(Request(url,headers={"User-Agent":"chicago-taxi-analytics/1.0"}),timeout=90) as response:return json.load(response)
        except (HTTPError,URLError,TimeoutError) as exc:
            if isinstance(exc,HTTPError) and exc.code not in (429,500,502,503,504):raise
            if attempt==4:raise
            time.sleep(2**attempt)


def normalize_weather(response,start,end):
    hourly=response["hourly"];units=response["hourly_units"]
    for key,unit in {"temperature_2m":"°C","precipitation":"mm","snowfall":"cm"}.items():
        if units.get(key)!=unit:raise ValueError("Unexpected weather units: "+key)
    times=hourly["time"]
    if any(len(hourly[k])!=len(times) for k in VARIABLES):raise ValueError("Weather array length mismatch")
    result=[];seen=set()
    for i,ts in enumerate(times):
        utc=datetime.fromisoformat(ts)
        utc=utc.replace(tzinfo=timezone.utc) if utc.tzinfo is None else utc.astimezone(timezone.utc)
        if utc in seen or utc.minute or utc.second:raise ValueError("Duplicate/non-hourly weather timestamp")
        seen.add(utc);local=utc.astimezone(ZONE)
        values={key:hourly[key][i] for key in VARIABLES}
        for key,value in values.items():
            if value is not None and (not isinstance(value,(int,float)) or not math.isfinite(value) or (key!='temperature_2m' and value<0)):
                raise ValueError("Invalid weather measurement")
        if not start<=local.date()<end:continue
        payload=dict(values,hour_utc=utc.isoformat(),local_hour=local.strftime("%Y-%m-%dT%H:00:00"),model="era5",latitude=41.8781,longitude=-87.6298)
        result.append((utc,payload))
    return result


def weather_batches(start,end,fetch=get_json):
    # Request UTC margins so local end-day evening hours are included.
    cursor=start
    while cursor<end:
        next_date=min(cursor+timedelta(days=30),end)
        params=dict(latitude=41.8781,longitude=-87.6298,start_date=str(cursor-timedelta(days=1)),end_date=str(next_date),hourly=','.join(VARIABLES),timezone='GMT',models='era5')
        yield normalize_weather(fetch('https://archive-api.open-meteo.com/v1/archive?'+urlencode(params)),cursor,next_date)
        cursor=next_date

