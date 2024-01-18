from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import FLOAT, INTEGER, VARCHAR


# Creating connection string using config template and creating sqlalchemy engine
def createLocalEngine(user: str, pswrd: str, host: str, port: str, db: str):
    connectionString = (
        f"postgresql+psycopg2://{user}:{pswrd}@{host}:{port}/{db}"
    )

    # Define sql alchemy engine
    return create_engine(connectionString)


class fieldProcessing:
    """
    Class for parsing scraped dataframes and defining postgres datatypes before loading to database
    """

    # Parsing dataframes for corresponding fields and postgres datatypes
    def dataTypeParser(self, endpoint: str, fields):
        fieldsProcessed = [
            k for k, v in fields.items() if endpoint in v["type"]
        ]
        dataTypes = [
            v["dataType"] for k, v in fields.items() if endpoint in v["type"]
        ]

        return dict(zip(fieldsProcessed, dataTypes))

    def __init__(self, endpoint):
        # User-defined endpoint for field processing
        self.endpoint = endpoint

        # List of all endpoints that are available between fantasyPros ROS rankings and ESPM fantasy website
        self.allFields = ["qb", "rb", "wr", "te", "espnLeague"]

        # List of all fantasyPros-related endpoints that are available
        self.allFantasyProsFields = ["qb", "rb", "wr", "te"]

        # List of all fantasyPros-related basketball endpoints that are available
        self.allFantasyBasketballFields = [
            "overall",
            "pg",
            "sg",
            "sf",
            "pf",
            "c",
            "g",
            "f",
            "proBasketballRefStats",
        ]

        self.allFantasyProsBasketballFields = [
            "overall",
            "pg",
            "sg",
            "sf",
            "pf",
            "c",
            "g",
            "f",
        ]
        # List of all fields available in scraped dataframes
        self.fields = {
            "player_name": {"type": self.allFields, "dataType": VARCHAR(100)},
            "player_team_id": {
                "type": self.allFantasyProsFields,
                "dataType": VARCHAR(100),
            },
            "misc_fl": {
                "type": self.allFantasyProsFields,
                "dataType": FLOAT(),
            },
            "passing_att": {"type": ["qb"], "dataType": FLOAT()},
            "passing_cmp": {"type": ["qb"], "dataType": FLOAT()},
            "passing_yds": {"type": ["qb"], "dataType": FLOAT()},
            "passing_tds": {"type": ["qb"], "dataType": FLOAT()},
            "passing_ints": {"type": ["qb"], "dataType": FLOAT()},
            "rushing_att": {"type": ["qb", "rb", "wr"], "dataType": FLOAT()},
            "rushing_yds": {"type": ["qb", "rb", "wr"], "dataType": FLOAT()},
            "rushing_tds": {"type": ["qb", "rb", "wr"], "dataType": FLOAT()},
            "receiving_rec": {"type": ["rb", "wr", "te"], "dataType": FLOAT()},
            "receiving_yds": {"type": ["rb", "wr", "te"], "dataType": FLOAT()},
            "receiving_tds": {"type": ["rb", "wr", "te"], "dataType": FLOAT()},
            # Fantasy basketball fields
            "Player": {
                "type": self.allFantasyBasketballFields,
                "dataType": VARCHAR(100),
            },
            "Team": {
                "type": self.allFantasyProsBasketballFields,
                "dataType": VARCHAR(3),
            },
            "PTS": {
                "type": self.allFantasyBasketballFields,
                "dataType": FLOAT(),
            },
            "REB": {
                "type": self.allFantasyProsBasketballFields,
                "dataType": FLOAT(),
            },
            "AST": {
                "type": self.allFantasyBasketballFields,
                "dataType": FLOAT(),
            },
            "BLK": {
                "type": self.allFantasyBasketballFields,
                "dataType": FLOAT(),
            },
            "STL": {
                "type": self.allFantasyBasketballFields,
                "dataType": FLOAT(),
            },
            "FG%": {
                "type": self.allFantasyBasketballFields,
                "dataType": FLOAT(),
            },
            "FT%": {
                "type": self.allFantasyBasketballFields,
                "dataType": FLOAT(),
            },
            "3PM": {
                "type": self.allFantasyProsBasketballFields,
                "dataType": FLOAT(),
            },
            "GP": {
                "type": self.allFantasyProsBasketballFields,
                "dataType": FLOAT(),
            },
            "MIN": {
                "type": self.allFantasyProsBasketballFields,
                "dataType": FLOAT(),
            },
            "TO": {
                "type": self.allFantasyProsBasketballFields,
                "dataType": FLOAT(),
            },
            # ProBasketballReference only fields
            "Rk": {
                "type": ["proBasketballRefStats"],
                "dataType": VARCHAR(100),
            },
            "Pos": {
                "type": ["proBasketballRefStats"],
                "dataType": VARCHAR(100),
            },
            "Age": {"type": ["proBasketballRefStats"], "dataType": VARCHAR(2)},
            "Tm": {"type": ["proBasketballRefStats"], "dataType": VARCHAR(3)},
            "G": {"type": ["proBasketballRefStats"], "dataType": INTEGER()},
            "GS": {"type": ["proBasketballRefStats"], "dataType": INTEGER()},
            "MP": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "FG": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "FGA": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "3P": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "3PA": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "3P%": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "2P": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "2PA": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "2P%": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "eFG%": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "FT": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "FTA": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "ORB": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "DRB": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "TRB": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "TOV": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            "PF": {"type": ["proBasketballRefStats"], "dataType": FLOAT()},
            # Only ESPN fields
            "fantasy_team_name": {
                "type": ["espnLeague"],
                "dataType": VARCHAR(100),
            },
        }

        # Running datatype parser function on endpoint of interest
        self.dTypes = self.dataTypeParser(
            endpoint=self.endpoint, fields=self.fields
        )
