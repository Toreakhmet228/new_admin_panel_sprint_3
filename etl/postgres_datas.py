import psycopg2
import os
import logging
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
import backoff

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

def write_sql_file(path: str) -> str:
    with open(path, "r") as file:
        sql = file.read()
    return sql

@backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_time=300)
def get_data_from_postgresql(batch_size=5000):
    dsl = {
        "dbname": os.environ.get("POSTGRES_DB"),
        "user": os.environ.get("POSTGRES_USER"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "host": "127.0.0.1",
        "port": os.environ.get("SQL_PORT", 5432)
    }

    connect = psycopg2.connect(**dsl)
    cursor = connect.cursor()

    cursor.execute(write_sql_file("sql/get_datas.sql"))

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows

    cursor.close()
    connect.close()

def transform_data_for_elasticsearch(rows):
    for row in rows:
        if len(row) < 10:  
            logger.error(f"Неверный формат данных: {row}")
            continue
        try:
            doc = {
                "_index": "movies",
                "_id": row[0], 
                "_source": {
                    "imdb_rating": row[1],  
                    "title": row[2],  
                    "description": row[3], 
                    "directors_names": row[4],  
                    "actors_names": row[5],  
                    "writers_names": row[6],  
                    "directors": [{"id": dir_id, "name": dir_name} for dir_id, dir_name in row[7]],
                    "actors": [{"id": act_id, "name": act_name} for act_id, act_name in row[8]],
                    "writers": [{"id": wr_id, "name": wr_name} for wr_id, wr_name in row[9]],
                }
            }
            yield doc
        except IndexError as e:
            logger.error(f"Ошибка трансформации данных: {e}, строка: {row}")

@backoff.on_exception(backoff.expo, Exception, max_time=300)
def load_data_to_elasticsearch(data):
    helpers.bulk(es, data)
    logger.info("Данные успешно загружены в Elasticsearch")

if __name__ == "__main__":
    try:
        for rows in get_data_from_postgresql():
            transformed_data = list(transform_data_for_elasticsearch(rows))
            load_data_to_elasticsearch(transformed_data)

    except Exception as e:
        logger.error(f"Ошибка выполнения ETL-процесса: {e}")
