import pandas as pd
import requests
import json
from unidecode import unidecode
from datetime import datetime
from espn_api.football import League
from espn_api.basketball import League

import os
from dotenv import load_dotenv


def getFootballProjections(pos: str):
    """
    Scrape fantasy football projections from fantasyPros website
    """
    if pos == "qb":
        fantasyProsUrl = f"https://www.fantasypros.com/nfl/projections/{pos}.php"

    else:
        fantasyProsUrl = (
            f"https://www.fantasypros.com/nfl/projections/{pos}.php?scoring=PPR"
        )

    try:
        # Making request to fantasyPros url
        players = pd.read_html(fantasyProsUrl)[0]

        # Running some quick transformations on players data frame
        players.columns = [
            "_".join(col).lower() for col in players.columns.to_flat_index()
        ]
        players[["player_name", "player_team_id"]] = players[
            "unnamed: 0_level_0_player"
        ].str.rsplit(" ", 1, expand=True)
        players = players.drop(["unnamed: 0_level_0_player", "misc_fpts"], axis=1)

        print(f"Projections for {pos} successfully scraped!")

        return players

    except Exception as e:
        print("Webscraping error:", str(e))


def getBasketballProjections(pos: str):
    """
    Scrape fantasy basketball projections from fantasyPros website
    """
    fProsUrl = f"https://www.fantasypros.com/nba/projections/avg-ros-{pos}.php"

    try:
        playersDf = pd.read_html(fProsUrl)[0]
        playersDf["Team"] = (
            playersDf["Player"].str.split("\(").str[1].str.split(" \-").str[0]
        )
        playersDf["Player"] = playersDf["Player"].str.split(" \(").str[0]

        return playersDf

    except Exception as e:
        print("Webscraping error:", str(e))


def getProBasketballReferenceStats(year: str):
    """
    Scrape stats from probasketballreference to obtain variables needed for some fantasy projects (i.e. '3PA' for 3-Point Efficiency)
    """
    proBasketballRefUrl = (
        f"https://www.basketball-reference.com/leagues/NBA_{year}_per_game.html"
    )

    try:
        playersDf = pd.read_html(proBasketballRefUrl)[0]
        playersDf = playersDf[playersDf["Rk"] != "Rk"].fillna(0.0)
        playersDf = playersDf.drop_duplicates(subset=["Player"], keep="first")
        playersDf["Player"] = playersDf["Player"].apply(lambda x: unidecode(x))

        return playersDf

    except Exception as e:
        print("Webscraping error:", str(e))


def getEspnPlayers(leagueId: str, leagueYear: int, espnS2: str, espnSwid: str):
    """
    Pull player names and associated team roster and owner in fantasy football league
    """
    teams = []
    league = League(
        league_id=leagueId,
        year=leagueYear,
        espn_s2=espnS2,
        swid=espnSwid,
    )

    for i in range(0, len(league.teams)):
        team = [
            (
                league.teams[i].__dict__["team_name"],
                p,
            )
            for p in league.teams[i].__dict__["roster"]
        ]
        teamDf = pd.DataFrame(team, columns=["fantasy_team_name", "player_name"])
        teams.append(teamDf)

    players = pd.concat(teams, axis=0, ignore_index=True)
    players["player_name"] = players["player_name"].astype(str).str[7:-1]

    return players


if __name__ == "__main__":
    # Loading environmental variables from .env
    load_dotenv()

    # Config
    espnLeague = os.environ.get("ESPN_LEAGUEID")
    espnYear = int(os.environ.get("ESPN_YEAR"))
    espnSwids = os.environ.get("ESPN_SWID")
    espnS2s = os.environ.get("ESPN_S2")

    pg = getBasketballProjections("pg")
    pbrStats = getProBasketballReferenceStats("2024")

    print(pg.head())
    print(pbrStats.columns)
    print(pbrStats.head())
