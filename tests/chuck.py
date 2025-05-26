import polars as pl

from rev_connectors import chuck


# Make a call to the Chuck Norris API for a random joke
# Return as a DataFrame
chuck_joke_df = pl.DataFrame().chuck.query()
print(chuck_joke_df)
