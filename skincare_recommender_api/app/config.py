from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    snowflake_user: str
    snowflake_password: str
    snowflake_account: str
    snowflake_warehouse: str = "COMPUTE_WH"
    snowflake_database: str = "GLOWPIPE_DB"
    snowflake_schema: str = "GOLD"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()