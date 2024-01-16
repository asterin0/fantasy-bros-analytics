import pytest
import os
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER, FLOAT, BOOLEAN
from fantasyBros.scripts.scrapeFantasyPros import (
    getFootballProjections,
    getBasketballProjections,
    getProBasketballReferenceStats,
    getEspnPlayers,
)
from fantasyBros.utils.devSetupLocal import fieldProcessing

# Loading environmental variables from .env
load_dotenv()


class testFantasyBrosScraper:
    def __init__(self):
        # Config
        self.espnLeague = os.environ.get("ESPN_LEAGUEID")
        self.espnYear = int(os.environ.get("ESPN_YEAR"))
        self.espnSwid = os.environ.get("ESPN_SWID")
        self.espnS2 = os.environ.get("ESPN_S2")

    def testPlayerCount(self, position):
        # Defining different logic by fantasysport
        if position in ["qb", "rb", "wr", "te"]:  # FantasyPros Football
            scraperDf = getFootballProjections(position)
            players = scraperDf["player_name"].values
        elif position in [
            "overall",
            "pg",
            "sg",
            "sf",
            "pf",
            "c",
            "g",
            "f",
        ]:  # FantasyPros Basketball
            scraperDf = getBasketballProjections(position)
            players = scraperDf["Player"].values

        elif position == "proBasketballRefStats":  # Pro Basketball Reference Stats
            scraperDf = getProBasketballReferenceStats("2024")
            players = scraperDf["Player"].values

        assert len(players) > 0

    def testEspnLeaguePlayerCount(self):
        scraperDf = getEspnPlayers(
            leagueId=self.espnLeague,
            leagueYear=self.espnYear,
            espnS2=self.espnS2,
            espnSwid=self.espnSwid,
        )
        players = scraperDf["player_name"].values
        assert len(players) > 0

    def testFieldCounts(self, position):
        """
        Determines if we have cross platform differences in field counts
        """
        metadata = fieldProcessing(position)

        # Defining different logic by fantasysport
        if position in ["qb", "rb", "wr", "te"]:  # FantasyPros Football
            scraperDf = getFootballProjections(position)

        elif position in [
            "overall",
            "pg",
            "sg",
            "sf",
            "pf",
            "c",
            "g",
            "f",
        ]:  # FantasyPros Basketball
            scraperDf = getBasketballProjections(position)

        elif position == "proBasketballRefStats":  # Pro Basketball Reference Stats
            scraperDf = getProBasketballReferenceStats("2024")

        # Using sets to identify fields in scraped column list that are not currently taken into account
        controllerFields = set(list(metadata.dTypes.keys()))
        webFields = set(scraperDf.columns)
        unmatched = webFields.symmetric_difference(controllerFields)

        assert (
            len(unmatched) == 0
        ), f"{position} field counts do not match between web and fields defined in controller - {unmatched} missing from controller fields"

    def testFieldLength(self, position):
        """
        Compares column lengths cross platform for any differences
        """
        metadata = fieldProcessing(position)
        # Defining different logic by fantasysport
        if position in ["qb", "rb", "wr", "te"]:  # Football
            scraperDf = getFootballProjections(position)

        elif position in [
            "overall",
            "pg",
            "sg",
            "sf",
            "pf",
            "c",
            "g",
            "f",
        ]:  # Basketball
            scraperDf = getBasketballProjections(position)

        elif position == "proBasketballRefStats":  # Pro Basketball Reference Stats
            scraperDf = getProBasketballReferenceStats("2024")

        # Creating dictionaries for max value length in web columns and currently specified column lengths respectively for comparison
        webColLengths = {
            col: max(scraperDf[col].astype(str).apply(len)) for col in scraperDf.columns
        }
        controllerColLengths = {}

        for key, val in metadata.dTypes.items():
            if type(val) == VARCHAR:
                controllerColLengths[key] = val.length
            elif type(val) in [INTEGER, FLOAT]:
                controllerColLengths[key] = 64
            elif type(val) == BOOLEAN:
                controllerColLengths[key] = 5

        # Checking max value lengths in web againts currently specified column value lengths
        for key in webColLengths.keys():
            assert (
                webColLengths[key] <= controllerColLengths[key]
            ), f"{key} did not pass value length check."


@pytest.fixture(scope="session")
def testWebScraperFixture():
    return testFantasyBrosScraper()