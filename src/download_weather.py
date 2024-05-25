import pandas as pd
import requests
import json
import itertools

URL = "https://api.weather.com/v1/location/KORD:9:US/observations/historical.json?apiKey=e1f10a1e78da46f5b10a1e78da96f525&units=e&startDate={}&endDate={}"


years = ["2020", "2021", "2022", "2023", "2024"]

start_dates = [
    "0101",
    "0201",
    "0302",
    "0401",
    "0501",
    "0601",
    "0701",
    "0801",
    "0901",
    "1001",
    "1101",
    "1201",
]
end_dates = [
    "0131",
    "0301",
    "0331",
    "0430",
    "0531",
    "0630",
    "0731",
    "0831",
    "0930",
    "1031",
    "1130",
    "1231",
]


def download_dates(start, end):
    resp = requests.get(URL.format(start, end))
    d = json.loads(resp.content)
    return d


def download_2023():
    ldf = []
    for year, (start, end) in itertools.product(years, zip(start_dates, end_dates)):
        if year == "2024" and start == "0401":
            break
        d = download_dates(year + start, year + end)
        df = pd.DataFrame(d["observations"])
        df.loc[:, "valid_time_gmt_dt"] = pd.to_datetime(df.valid_time_gmt, unit="s")
        ldf.append(df)
    bdf = pd.concat(ldf)
    bdf.to_parquet("data/finished/chicago_2020_23_weather.parquet")


if __name__ == "__main__":
    download_2023()
