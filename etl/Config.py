from pydantic import BaseSettings
import os
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    db_name: str = os.environ.get("POSTGRES_DB")
    db_user: str = os.environ.get("POSTGRES_USER")
    db_password: str = os.environ.get("POSTGRES_PASSWORD")
    db_host: str = os.environ.get("POSTGRES_HOST")
    db_port: int = os.environ.get("POSTGRES_PORT", 5432)
    
    es_host: str = os.environ.get("ELASTIC_HOST")
    es_port: int = os.environ.get("ELASTIC_PORT", 9200)
    es_scheme: str = os.environ.get("ELASTIC_SCHEME", 'http')

    class Config:
        env_file = '.env'
        env_prefix = 'POSTGRES_'  # Префикс для переменных окружения, относящихся к Postgres

settings = Settings()

es = Elasticsearch([{
    'host': settings.es_host,
    'port': settings.es_port,
    'scheme': settings.es_scheme
}])
