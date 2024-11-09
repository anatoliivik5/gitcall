from pydantic_settings import BaseSettings


class Settings(BaseSettings):
	beeline_api_url: str
	beeline_api_auth_token: str

	class Config:
		env_file = ".env"
		env_file_encoding = "utf-8"


settings = Settings()
