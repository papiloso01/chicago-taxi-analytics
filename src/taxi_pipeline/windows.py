from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def daily_window(today=None):
    today = today or datetime.now(ZoneInfo("America/Chicago")).date()
    return str(today - timedelta(days=1)), str(today)


def year_window(year):
    if not 2024 <= year < date.today().year:
        raise ValueError("Choose a completed calendar year from 2024 onward")
    return f"{year}-01-01", f"{year+1}-01-01"
