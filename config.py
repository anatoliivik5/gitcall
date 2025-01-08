from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Явная загрузка .env
load_dotenv()

class Settings(BaseSettings):
    beeline_api_url: str
    beeline_api_auth_token: str
    bitrix_url: str
    bitrix_api_key: str

    class Config:
        # Указываем явно путь к .env
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
