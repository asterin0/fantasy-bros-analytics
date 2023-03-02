from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER, FLOAT, BOOLEAN


class fieldProcessing:
    """
    Class for parsing scraped dataframes and defining postgres datatypes before loading to database
    """

    # Parsing dataframes for corresponding fields and postgres datatypes
    def dataTypeParser(self, endpoint: str, fields):
        fieldsProcessed = [k for k, v in fields.items() if endpoint in v["type"]]
        dataTypes = [v["dataType"] for k, v in fields.items() if endpoint in v["type"]]

        return dict(zip(fieldsProcessed, dataTypes))

    def __init__(self, endpoint):
        # User-defined endpoint for field processing
        self.endpoint = endpoint

        # List of all fantasyPros-related basketball endpoints that are available
        self.allFantasyBasketballFields = [
            "allPlayers",
            "pointGuards",
            "shootingGuards",
            "smallForwards",
            "powerForwards",
            "centers",
            "guards",
            "forwards",
        ]

        # List of all fields available in scraped dataframes
        self.fields = {
            "Player": {
                "type": self.allFantasyBasketballFields,
                "dataType": VARCHAR(100),
            },
            "Team": {"type": self.allFantasyBasketballFields, "dataType": VARCHAR(100)},
            "PTS": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            "REB": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            "AST": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            "BLK": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            "STL": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            "FG%": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            "FT%": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            "3PM": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            "GP": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            "MIN": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            "TO": {"type": self.allFantasyBasketballFields, "dataType": FLOAT()},
            # ProBasketballReference only fields
            "Rk": {"type": "proBasketballRefStats", "dataType": VARCHAR(100)},
            "Player": {"type": "proBasketballRefStats", "dataType": VARCHAR(100)},
            "Pos": {"type": "proBasketballRefStats", "dataType": VARCHAR(100)},
            "Age": {"type": "proBasketballRefStats", "dataType": VARCHAR(2)},
            "Tm": {"type": "proBasketballRefStats", "dataType": VARCHAR(3)},
            "G": {"type": "proBasketballRefStats", "dataType": INTEGER()},
            "GS": {"type": "proBasketballRefStats", "dataType": INTEGER()},
            "MP": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "FG": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "FGA": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "FG%": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "3P": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "3PA": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "3P%": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "2P": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "2PA": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "2P%": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "eFG%": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "FT": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "FTA": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "FT%": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "ORB": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "DRB": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "TRB": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "AST": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "STL": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "BLK": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "TOV": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "PF": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            "PTS": {"type": "proBasketballRefStats", "dataType": FLOAT()},
            # Only ESPN fields
            # "fantasy_team_name": {"type": ["espnLeague"], "dataType": VARCHAR(100)},
            # "owner_name": {"type": ["espnLeague"], "dataType": VARCHAR(100)},
        }

        # Running datatype parser function on endpoint of interest
        self.dTypes = self.dataTypeParser(endpoint=self.endpoint, fields=self.fields)
