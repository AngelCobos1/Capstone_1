import time

import pandas as pd
import requests
from sqlalchemy import create_engine

# Create function to pull every season


def sourcing_NBA(years):
    time.sleep(1)  # one second delay on each pull request
    source = []  # create empty list
    for i in years:  # iterate through all years in input
        headers = {
            "Connection": "keep-alive",
            "Accept": "application/json, text/plain, */*",
            "x-nba-stats-token": "true",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36",
            "x-nba-stats-origin": "stats",
            "Origin": "https://www.nba.com",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://www.nba.com/",
            "Accept-Language": "en-US,en;q=0.9",
        }

        params = (
            ("College", ""),
            ("LeagueID", "00"),
            ("OverallPick", ""),
            ("RoundNum", ""),
            ("RoundPick", ""),
            ("Season", i),  # this is where the magic happens
            ("TeamID", "0"),
            ("TopX", ""),
        )

        source.append(
            requests.get(
                "https://stats.nba.com/stats/drafthistory",
                headers=headers,
                params=params,
            ).json()
        )  # append every request to the empty source list
    return source


# NB. Original query string below. It seems impossible to parse and
# reproduce query strings 100% accurately so the one below is given
# in case the reproduced version is not "correct".
# response = requests.get('https://stats.nba.com/stats/drafthistory?College=&LeagueID=00&OverallPick=&RoundNum=&RoundPick=&Season=2020&TeamID=0&TopX=', headers=headers)


def make_df(years):
    # previous function to extract every season of drafts
    data = sourcing_NBA(years)
    filtered_list = [data[i]["resultSets"][0]["rowSet"] for i in range(0, 11)]
    df = pd.concat([pd.DataFrame(filtered_list[i]) for i in range(0, 11)], axis=0)

    column_names = data[0]["resultSets"][0]["headers"]
    df.columns = column_names
    return df


years = [str(i) for i in range(2010, 2021)]

NBA_Drafts = make_df(years)

NBA_Drafts.to_csv(r"C:\Users\ASUS\Thinkful_Projects\Capstone_1\NBA_Drafts.csv")

# PostgreSQL needs the columns names lower-cased
NBA_Drafts.columns = NBA_Drafts.columns.str.lower()

# Create engine to SQL database
engine = create_engine("postgresql://postgres:mybigdata@localhost:5433/nba")
NBA_Drafts.to_sql("drafts", con=engine, if_exists="append", index=False)
engine.dispose()
