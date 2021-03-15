import time

import pandas as pd
import requests
from sqlalchemy import create_engine


def request_seasons(seasons):
    time.sleep(1)  # one second delay on each pull request
    source = []  # create empty list
    for i in seasons:  # iterate through all years in input
        headers = {
            "Connection": "keep-alive",
            "Accept": "application/json, text/plain, */*",
            "x-nba-stats-token": "true",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
            "x-nba-stats-origin": "stats",
            "Origin": "https://www.nba.com",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://www.nba.com/",
            "Accept-Language": "en-US,en;q=0.9",
            "If-Modified-Since": "Sun, 14 Feb 2021 03:05:20 GMT",
        }

        params = (
            ("College", ""),
            ("Conference", ""),
            ("Country", ""),
            ("DateFrom", ""),
            ("DateTo", ""),
            ("Division", ""),
            ("DraftPick", ""),
            ("DraftYear", ""),
            ("GameScope", ""),
            ("GameSegment", ""),
            ("Height", ""),
            ("LastNGames", "0"),
            ("LeagueID", "00"),
            ("Location", ""),
            ("MeasureType", "Base"),
            ("Month", "0"),
            ("OpponentTeamID", "0"),
            ("Outcome", ""),
            ("PORound", "0"),
            ("PaceAdjust", "N"),
            ("PerMode", "PerGame"),
            ("Period", "0"),
            ("PlayerExperience", ""),
            ("PlayerPosition", ""),
            ("PlusMinus", "N"),
            ("Rank", "N"),
            ("Season", i),
            ("SeasonSegment", ""),
            ("SeasonType", "Regular Season"),
            ("ShotClockRange", ""),
            ("StarterBench", ""),
            ("TeamID", "0"),
            ("TwoWay", "0"),
            ("VsConference", ""),
            ("VsDivision", ""),
            ("Weight", ""),
        )

        source.append(
            requests.get(
                "https://stats.nba.com/stats/leaguedashplayerstats",
                headers=headers,
                params=params,
            ).json()
        )
    return source


# NB. Original query string below. It seems impossible to parse and
# reproduce query strings 100% accurately so the one below is given
# in case the reproduced version is not "correct".
# response = requests.get('https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2020-21&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision=&Weight=', headers=headers)


seasons = [
    "2000-01",
    "2001-02",
    "2002-03",
    "2003-04",
    "2004-05",
    "2005-06",
    "2006-07",
    "2007-08",
    "2008-09",
    "2009-10",
    "2010-11",
    "2011-12",
    "2012-13",
    "2013-14",
    "2014-15",
    "2015-16",
    "2016-17",
    "2017-18",
    "2018-19",
    "2019-20",
    "2020-21",
]

# def request_seasons(years):
# previous function to extract every season of drafts
data = request_seasons(seasons)

# Create function to append NBA Season to first player row within stats list, as the last element in list


def insert_seasons(nested_list):
    return [
        data[i]["resultSets"][0]["rowSet"][0].append((data[i]["parameters"]["Season"]))
        for i in range(0, 11)
    ]


insert_seasons(data)


def make_df(data):
    filtered_list = [data[i]["resultSets"][0]["rowSet"] for i in range(0, 11)]
    df = pd.concat([pd.DataFrame(filtered_list[i]) for i in range(0, 11)], axis=0)
    # Rename columns
    column_names = data[0]["resultSets"][0]["headers"] + ["SEASON"]
    df.columns = column_names
    # Forward-fill the null values in the season column
    df.loc[:, ["SEASON"]] = df.loc[:, ["SEASON"]].ffill()
    return df


NBA_Stats = make_df(data)

NBA_Stats.to_csv(r"C:\Users\ASUS\Thinkful_Projects\Capstone_1\NBA_Drafts.csv")

# PostgreSQL needs the columns names lower-cased
NBA_Stats.columns = NBA_Stats.columns.str.lower()

# Create engine to SQL database
engine = create_engine("postgresql://postgres:mybigdata@localhost:5433/nba")
NBA_Stats.to_sql("stats", con=engine, if_exists="append", index=False)
engine.dispose()
