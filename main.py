from YTS_Consumer.YTSConsumer import YTSConsumer
from pymongo import MongoClient

BASE_URL = "https://yts.mx/api/v2/list_movies.json?limit={}&page={}"
LIMIT = 50


def start_consuming() -> None:
    page = 1
    client = MongoClient(host='localhost', port=27017)
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
    start_consuming()
