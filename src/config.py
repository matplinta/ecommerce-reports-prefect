from pydantic import (
    PostgresDsn,
    computed_field,
)
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

# import os
# print(f"Current working directory: {os.getcwd()}")
# print(f"Looking for .env at: {os.path.abspath(os.path.join(os.getcwd(), '../.env'))}")

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # Use top level .env file from src/db pov (one level above ./src/)
        env_file="../../.env",
        env_ignore_empty=True,
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    POSTGRES_DB_URI: str = "postgresql+psycopg2://dev:secret@localhost:5432/shop"

    # POSTGRES_SERVER: str
    # POSTGRES_PORT: int = 5432
    # POSTGRES_USER: str
    # POSTGRES_PASSWORD: str = ""
    # POSTGRES_DB: str = ""
    
    # @computed_field  # type: ignore[prop-decorator]
    # @property
    # def SQLALCHEMY_DATABASE_URI(self) -> PostgresDsn:
    #     return MultiHostUrl.build(
    #         scheme="postgresql+psycopg",
    #         username=self.POSTGRES_USER,
    #         password=self.POSTGRES_PASSWORD,
    #         host=self.POSTGRES_SERVER,
    #         port=self.POSTGRES_PORT,
    #         path=self.POSTGRES_DB,
    #     )


settings = Settings()  # type: ignore

# Function to override settings programmatically
def update_settings(**kwargs):
    for key, value in kwargs.items():
        if hasattr(settings, key):
            setattr(settings, key, value)