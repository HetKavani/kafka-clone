from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "KafkaX"
    API_V1_STR: str = "/api/v1"
    
    # DB URL default is for docker compose, can be overridden by environment variable
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@postgres:5432/kafkax"
    REDIS_URL: str = "redis://redis:6379/0"

    # Distributed configurations
    BROKER_ID: Optional[str] = None
    HOST: str = "localhost"
    PORT: int = 8000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

settings = Settings()

