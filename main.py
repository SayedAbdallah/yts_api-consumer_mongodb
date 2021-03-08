import configparser
from YTS_Consumer.YTSConsumer import YTSConsumer
from pymongo import MongoClient
from typing import Dict
import json

BASE_URL = "https://yts.mx/api/v2/list_movies.json?limit={}&page={}"
LIMIT = 50


def read_config() -> Dict:
    conf = configparser.ConfigParser()
    conf.read('yts.ini')
    return dict(conf['DEFAULT'].items())


def _format_movie(movie: Dict) -> Dict:
    new_movie = {
        'id': movie['id'],
        'url': movie.get('url'),
        'imdb_code': movie.get('imdb_code'),
        'title': movie.get('title'),
        'title_english': movie.get('title_english'),
        'title_long': movie.get('title_long'),
        'slug': movie.get('slug'),
        'release_year': movie.get('year'),
        'rating': movie.get('rating'),
        'runtime': movie.get('runtime'),
        'synopsis': movie.get('synopsis'),
        'trailer_code': movie.get('yt_trailer_code'),
        'language': movie.get('language'),
        'mpa_rating': movie.get('mpa_rating'),
        'genres': movie.get('genres', []),
        'torrents': movie.get('torrents', [])
    }

    return new_movie


def start_consuming(config: Dict) -> None:
    page = 1
    client = MongoClient(host=config['host'], port=int(config['port']))
    db = client.yts

    while True:
        try:
            print(f'Start to scrap page {page}')
            url = BASE_URL.format(LIMIT, page)
            movies = YTSConsumer.consume(url=url)
            movies = list(map(_format_movie, movies))
            result = db.movies.insert_many(documents=movies)
            print(f'{len(result.inserted_ids)} Movies inserted')
            page += 1

        except KeyError:
            print('Finish Scrapping all Movies')
            break


if __name__ == '__main__':
    config = read_config()
    start_consuming(config=config)
