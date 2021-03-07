import configparser
from YTS_Consumer.YTSConsumer import YTSConsumer
from pymongo import MongoClient
from typing import Dict


BASE_URL = "https://yts.mx/api/v2/list_movies.json?limit={}&page={}"
LIMIT = 50


def read_config() -> Dict:
    conf = configparser.ConfigParser()
    conf.read('yts.ini')
    return dict(conf['DEFAULT'].items())


def start_consuming(config: Dict) -> None:
    page = 1
    client = MongoClient(host=config['host'], port=int(config['port']))
    db = client.yts

    while True:
        try:
            print(f'Start to scrap page {page}')
            url = BASE_URL.format(LIMIT, page)
            movies = YTSConsumer.consume(url=url)
            result = db.movies.insert_many(documents=movies)
            print(f'{len(result.inserted_ids)} Movies inserted')
            page += 1

        except KeyError:
            print('Finish Scrapping all Movies')
            break


if __name__ == '__main__':
    config = read_config()
    start_consuming(config=config)
