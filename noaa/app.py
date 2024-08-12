import os
import json
import requests
from datetime import datetime


def reformat_date(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    return date_obj.strftime("%Y-%m-%d")


def make_noaa_req(opts={}):
    try:
        token = os.environ["NOAA_API_KEY"]
    except KeyError as e:
        err = "Error: NOAA_API_KEY not found in environment, can't fetch data."
        return {"error": err}

    start_date = f"{opts['year']}-01-01"
    end_date = f"{opts['year']}-12-31"
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"

    params = {
        "stationid": opts["stationid"],
        "datatypeid": "TMAX",
        "limit": opts["limit"],
        "datasetid": "GHCND",
        "startdate": start_date,
        "enddate": end_date,
        "units": "standard",  # 🇺🇸
    }

    headers = {"token": token}
    response = requests.get(url, headers=headers, params=params)
    data = json.loads(response.text)

    if "results" in data:
        for obj in data["results"]:
            formatted_date = reformat_date(obj["date"])
            obj["date"] = formatted_date
            obj["id"] = f"{opts['stationid']}-{formatted_date}"

    return data


def lambda_handler(event, context):
    state = event.get("state", {})
    curr_year = datetime.now().year
    requested_year = state.get("year", curr_year)
    year = requested_year if requested_year <= curr_year else curr_year

    # TODO: Multiple stations theoretically work, but I haven't been able to get data back from the API yet
    stations = state.get("stations", ["GHCND:USW00013904"])
    opts = {
        "year": year,
        "stationid": "&".join(stations),
        "limit": state.get("limit", 366),
    }

    noaa_data = make_noaa_req(opts)

    if "error" in noaa_data:
        print(noaa_data["error"])
        return json.dumps({})

    # Move forward a year if we're not there already. This will come back in the next invocation
    next_year = year + 1 if year < curr_year else year

    ret = {
        "state": {"year": next_year},
        "insert": {"max_temp": noaa_data.get("results", [])},
        "schema": {
            "max_temp": {"primary_key": ["id"]},
        },
        "hasMore": False,
    }
    return json.dumps(ret)


if __name__ == "__main__":
    # NOTE: Right now, this is only used for local testing, so use a limited state
    lambda_handler({"state": {"year": 2020, "limit": 1}}, {})
