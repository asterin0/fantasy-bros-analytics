import pytest


# Defining positions to test
positions = [
    # Football positions
    "qb",
    "rb",
    "wr",
    "te",
    # Basketball positions
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


@pytest.mark.parametrize("position", positions)
def testPlayerCount(position, testWebScraperFixture):
    testWebScraperFixture.testPlayerCount(position=position)


@pytest.mark.parametrize("position", positions)
def testFieldCount(position, testWebScraperFixture):
    testWebScraperFixture.testFieldCounts(position=position)


@pytest.mark.parametrize("position", positions)
def testColLength(position, testWebScraperFixture):
    testWebScraperFixture.testFieldLength(position=position)
