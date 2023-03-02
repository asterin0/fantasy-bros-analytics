import pandas as pd
import asyncio
import requests
import json
from unidecode import unidecode
from app.fantasyDataIngestion.databaseSetup import createEngine
from app.fantasyDataIngestion.dataTypeControllers import fieldProcessing
from datetime import datetime

# from espn_api.basketball import League


class webScraper:
    """
    Class for scraping fantasyPros projections from webpages and sending to postgres database
    """

    def __init__(self):

        self.engine = createEngine()

    async def getBasketballProjections(self, pos: str):

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

    async def getProBasketballReferenceStats(self, year: str):

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

    async def sendDataToPostgres(self, df, table_name: str):

        engine = self.engine
        conn = engine.connect()

        metaData = fieldProcessing(endpoint=table_name)

        tableExistCheck = conn.execute(
            f"""SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE  table_schema = 'staging'
                AND    table_name   = '{table_name}'
                );"""
        ).fetchone()[0]

        if tableExistCheck:
            conn.execute(f'TRUNCATE TABLE "staging"."{table_name}"')

        print(f"Sending {table_name} data to postgres")

        try:
            df.to_sql(
                name=table_name,
                con=engine,
                schema="staging",
                if_exists="append",
                method="multi",
                dtype=metaData.dTypes,
            )

            print(f"Successfully sent {table_name} data to postgres!")

        except Exception as e:
            print("Could not send data to postgres: ", str(e))

    def asyncEtl(self):
        initTime = datetime.now()

        async def main(self):

            allPlayers = await self.getBasketballProjections("overall")
            pointGuards = await self.getBasketballProjections("pg")
            shootingGuards = await self.getBasketballProjections("sg")
            smallForwards = await self.getBasketballProjections("sf")
            powerForwards = await self.getBasketballProjections("pf")
            centers = await self.getBasketballProjections("c")
            guards = await self.getBasketballProjections("g")
            forwards = await self.getBasketballProjections("f")
            proBasketballRefStats = await self.getProBasketballReferenceStats("2023")
            # espnLeague = await self.getEspnPlayers()

            await asyncio.gather(
                self.sendDataToPostgres(df=allPlayers, table_name="allPlayers"),
                self.sendDataToPostgres(df=pointGuards, table_name="pointGuards"),
                self.sendDataToPostgres(df=shootingGuards, table_name="shootingGuards"),
                self.sendDataToPostgres(df=smallForwards, table_name="smallForwards"),
                self.sendDataToPostgres(df=powerForwards, table_name="powerForwards"),
                self.sendDataToPostgres(df=centers, table_name="centers"),
                self.sendDataToPostgres(df=guards, table_name="guards"),
                self.sendDataToPostgres(df=forwards, table_name="forwards"),
                self.sendDataToPostgres(
                    df=proBasketballRefStats, table_name="proBasketballRefStats"
                ),
                # self.sendDataToPostgres(df=espnLeague, table_name="espnLeague"),
            )

        asyncio.run(main(self))

        finTime = datetime.now()
        print("ETL Execution time: ", finTime - initTime)


if __name__ == "__main__":
    scraper = webScraper()
    scraper.asyncEtl()
