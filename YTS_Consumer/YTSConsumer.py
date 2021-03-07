import requests
from typing import List, Dict


class YTSConsumer:

    @staticmethod
    def consume(url: str) -> List[Dict]:
        """
        Take URL and consume the API by requesting this URL
        Extract Movies Data from the Response
        :param url: the URL to request
        :return: List of Dictionaries each Dictionary represent a Movie
        """
        response = requests.get(url=url)
        data = response.json()
        try:
            movies = data['data']['movies']
            return movies
        except KeyError:
            raise KeyError('Can not find Movies Data in response.')
