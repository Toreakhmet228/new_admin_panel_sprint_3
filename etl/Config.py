from pydantic import BaseSettings
import os 
from dotenv import load_dotenv

class Settings(BaseSettings):
    db_name: str = os.environ.get("POSTGRES_DB")
    db_user: str =  os.environ.get("POSTGRES_USER")
    db_password: str = os.environ.get("POSTGRES_PASSWORD")
    db_host: str = os.environ.get("HOST"),
    db_port: int = os.environ.get("SQL_PORT", 5432)

    class Config:
        env_prefix = 'POSTGRES_'  
        env_file = '.env'  