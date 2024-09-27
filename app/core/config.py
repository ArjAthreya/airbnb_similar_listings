import sys
import sys
import logging
from typing import Optional

from loguru import logger
from pydantic_settings import BaseSettings
from pydantic import SecretStr
from core.logging import InterceptHandler

class Settings(BaseSettings):
    API_PREFIX: str = "/api"
    VERSION: str = "0.1.0"
    DEBUG: bool = False
    MAX_CONNECTIONS_COUNT: int = 10
    MIN_CONNECTIONS_COUNT: int = 10
    SECRET_KEY: SecretStr = SecretStr("")
    PROJECT_NAME: str = "airbnb_similar_listings"

    LOGGING_LEVEL: int = logging.INFO

    INPUT_EXAMPLE: str = "./ml/model/examples/example.json"

    DB_PATH: str = "./airbnb.db"

    class Config:
        env_file = ".env"

# Create an instance of the Settings class
settings = Settings()

# Configure logging
LOGGING_LEVEL = logging.DEBUG if settings.DEBUG else settings.LOGGING_LEVEL
logging.basicConfig(
    handlers=[InterceptHandler(level=LOGGING_LEVEL)], level=LOGGING_LEVEL
)
logger.configure(handlers=[{"sink": sys.stderr, "level": LOGGING_LEVEL}])
