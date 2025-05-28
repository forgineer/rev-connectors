import polars as pl
import requests

# Chuck Norris Joke API Connector
# This is connector is for demonstration purposes only to help guide the creation of new connectors.
@pl.api.register_dataframe_namespace('chuck')
class Chuck():
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df
        self._base_url = 'https://api.chucknorris.io/jokes'
        
    def categories(self) -> pl.DataFrame:
        categories = requests.get(f'{self._base_url}/categories')
        return pl.DataFrame(categories.json())

    def random(self) -> pl.DataFrame:
        random_joke = requests.get(f'{self._base_url}/random')
        jokes: list[dict] = [random_joke.json()]
        return pl.DataFrame(jokes)

    def random_catetory(self, category: str) -> pl.DataFrame:
        random_category_joke = requests.get(f'{self._base_url}/random?category={category}')
        jokes: list[dict] = [random_category_joke.json()]
        return pl.DataFrame(jokes)
    
    def search(self, query: str) -> pl.DataFrame:
        search_jokes = requests.get(f'{self._base_url}/search?query={query}')
        jokes: list[dict] = search_jokes.json().get('result', [])
        return pl.DataFrame(jokes)
