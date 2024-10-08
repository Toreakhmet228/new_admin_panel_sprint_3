import logging
import os
from typing import List, Generator, Dict

import backoff
import psycopg2
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from pydantic import BaseSettings

from .Config import Settings

load_dotenv()
settings = Settings()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

es = Elasticsearch([{'host': settings.es_host, 'port': settings.es_port, 'scheme': settings.es_scheme}])

STATE_FILE = "etl_state.txt"

def write_sql_file(path: str) -> str:
    with open(path, "r") as file:
        sql = file.read()
    return sql

def save_state(last_processed_id: str):
    with open(STATE_FILE, "w") as file:
        file.write(last_processed_id)

def load_state() -> str:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as file:
            return file.read().strip()
    return None

@backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_time=300)
def get_data_from_postgresql(batch_size: int = 5000) -> Generator[List[Dict[str, any]], None, None]:
    dsl = {
        "dbname": settings.db_name,
        "user": settings.db_user,
        "password": settings.db_password,
        "host": settings.db_host,
        "port": settings.db_port,
    }

    connect = psycopg2.connect(**dsl)
    cursor = connect.cursor()

    last_processed_id = load_state()
    sql_query = write_sql_file("sql/get_datas.sql")
    
    if last_processed_id:
        sql_query += f" WHERE id > '{last_processed_id}'"
    
    cursor.execute(sql_query)

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows

    cursor.close()
    connect.close()

def transform_data_for_elasticsearch(rows: List[Dict[str, any]]) -> Generator[Dict[str, any], None, None]:
    for row in rows:
        required_keys = ["id", "imdb_rating", "title", "description", "directors_names", "actors_names", "writers_names", "directors", "actors", "writers"]
        
        if not all(key in row for key in required_keys):
            logger.error(f"Неверный формат данных: {row}")
            continue 
        
        try:
            doc = {
                "_index": "movies",
                "_id": row["id"],
                "_source": {
                    "imdb_rating": row["imdb_rating"],
                    "title": row["title"],
                    "description": row["description"],
                    "directors_names": row["directors_names"],
                    "actors_names": row["actors_names"],
                    "writers_names": row["writers_names"],
                    "directors": [{"id": dir_id, "name": dir_name} for dir_id, dir_name in row["directors"]],
                    "actors": [{"id": act_id, "name": act_name} for act_id, act_name in row["actors"]],
                    "writers": [{"id": wr_id, "name": wr_name} for wr_id, wr_name in row["writers"]],
                }
            }
            yield doc
        except KeyError as e:
            logger.error(f"Ошибка трансформации данных: отсутствует ключ {e}, строка: {row}")

@backoff.on_exception(backoff.expo, Exception, max_time=300)
def load_data_to_elasticsearch(data):
    index_name = "movies"
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=index_settings)
        logger.info(f"Индекс '{index_name}' успешно создан.")
    else:
        logger.info(f"Индекс '{index_name}' уже существует.")
    helpers.bulk(es, data)
    logger.info("Данные успешно загружены в Elasticsearch")

if __name__ == "__main__":
    try:
        for rows in get_data_from_postgresql():
            transformed_data = list(transform_data_for_elasticsearch(rows))
            load_data_to_elasticsearch(transformed_data)
            # Сохраняем ID последней обработанной строки
            last_id = rows[-1]['id']
            save_state(last_id)

    except Exception as e:
        logger.error(f"Ошибка выполнения ETL-процесса: {e}")
