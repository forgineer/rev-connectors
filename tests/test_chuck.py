import polars as pl
import pytest
from unittest.mock import patch, MagicMock

from rev_connectors import chuck

@pytest.fixture
def empty_df():
    return pl.DataFrame()

@pytest.fixture
def chuck_ns(empty_df):
    return chuck.Chuck(empty_df)

def test_categories_returns_dataframe(chuck_ns):
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = ["animal", "career"]
        df = chuck_ns.categories()

        print(df)  # Debugging output to see the DataFrame structure

        assert isinstance(df, pl.DataFrame)

def test_random_returns_dataframe(chuck_ns):
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = {"id": "abc", "value": "joke"}
        df = chuck_ns.random()

        print(df)  # Debugging output to see the DataFrame structure

        assert isinstance(df, pl.DataFrame)
        assert "value" in df.columns

def test_random_catetory_returns_dataframe(chuck_ns):
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = {"id": "xyz", "value": "cat joke"}
        df = chuck_ns.random_catetory("animal")
        
        print(df)  # Debugging output to see the DataFrame structure

        assert isinstance(df, pl.DataFrame)


if __name__ == "__main__":
    random_joke = pl.DataFrame().chuck.random()
    print(random_joke)

    chuck_categories = pl.DataFrame().chuck.categories()
    print(chuck_categories)

    random_category_joke = pl.DataFrame().chuck.random_catetory("animal")
    print(random_category_joke)

    search_results = pl.DataFrame().chuck.search("chuck")
    print(search_results)