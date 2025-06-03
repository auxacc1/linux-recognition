import logging
from contextlib import contextmanager
from platform import system
from typing import Self

from anyio import Path
from pydantic import BaseModel, ConfigDict, Field, field_validator, NonNegativeInt, SecretStr
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource
)


class PostgresConfig(BaseModel):
    dbname: str | None = None
    user: str
    password: str = Field(exclude=True)
    host: str
    port: int

    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)

    def for_database(self, database_name: str) -> Self:
        return self.model_copy(update={'dbname': database_name})


class DatabaseSettings(BaseModel):
    recognized_db: str
    packages_db: str
    repology_db: str
    postgres_default: PostgresConfig
    postgres_udd: PostgresConfig

    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)


class LoggingAttributes(BaseModel):
    include_time: bool = True
    other: list[str] = Field(default_factory=list)

    # noinspection PyNestedDecorators
    @field_validator('other', mode='after')
    @classmethod
    def filter_out_time_attributes(cls, attributes: list[str]) -> list[str]:
        time_attributes = ['time', 'asctime']
        return [attr for attr in attributes if attr not in time_attributes]


class FileHandlerSettings(BaseModel):
    use: bool = True
    filename: str = 'log.json'


class LoggingSettings(BaseModel):
    log_to_console: bool = True
    file_handler: FileHandlerSettings = FileHandlerSettings()
    level: NonNegativeInt = logging.WARNING
    attributes: LoggingAttributes = LoggingAttributes()

    # noinspection PyNestedDecorators
    @field_validator('level', mode='before')
    @classmethod
    def retrieve_logging_level(cls, level: str | int) -> str | int:
        if isinstance(level, str):
            level_names_mapping = logging.getLevelNamesMapping()
            value_upper = level.upper()
            if value_upper in level_names_mapping:
                return level_names_mapping[value_upper]
            if level.isdecimal():
                return int(level)
        return level


class OpenAiModels(BaseModel):
    chat: str = Field(default='gpt-4o-mini')
    embeddings: str = Field(default='text-embedding-3-large')


class Settings(BaseSettings):
    database: DatabaseSettings
    logging: LoggingSettings
    openai: OpenAiModels


class Credentials(BaseSettings):
    pypi_user_agent: SecretStr = Field(exclude=True)
    sourceforge_bearer: SecretStr = Field(exclude=True)
    obs_username: SecretStr = Field(exclude=True)
    obs_password: SecretStr = Field(exclude=True)
    github_token: SecretStr = Field(exclude=True)
    openai_api_key: SecretStr = Field(exclude=True)


@contextmanager
def validated_credentials(env_file: Path, env_prefix: str):

    class CredentialsClass(Credentials):
        model_config = SettingsConfigDict(
            env_file=env_file,
            env_prefix=env_prefix,
            extra='ignore',
            hide_input_in_errors=True
        )

    CredentialsClass()
    yield


def initialize_settings(project_directory: Path) -> Settings:
    configuration_file = project_directory / 'config' / 'config.yaml'
    env_file = project_directory / '.env'
    env_prefix = 'LINUX_RECOGNITION__'

    class SettingsClass(Settings):
        model_config = SettingsConfigDict(
            yaml_file=configuration_file,
            env_file=env_file,
            env_prefix=env_prefix,
            env_nested_delimiter='__',
            extra='ignore',
            frozen=True,
            hide_input_in_errors=True,
        )

        @classmethod
        def settings_customise_sources(
                cls,
                settings_cls: type[BaseSettings],
                init_settings: PydanticBaseSettingsSource,
                env_settings: PydanticBaseSettingsSource,
                dotenv_settings: PydanticBaseSettingsSource,
                file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return env_settings, dotenv_settings, YamlConfigSettingsSource(settings_cls)

    with validated_credentials(env_file, env_prefix):
        return SettingsClass()


async def get_project_directory() -> Path:
    file_path = await Path(__file__).resolve()
    return file_path.parent


async def is_initialized() -> bool:
    data_directory = await _get_data_directory()
    return await (data_directory / 'initialized').exists()


async def mark_initialized():
    data_directory = await _get_data_directory()
    await data_directory.mkdir(parents=True, exist_ok=True)
    await (data_directory / 'initialized').touch(exist_ok=True)


async def _get_data_directory() -> Path:
    system_used = system()
    if system_used == 'Windows':
        data_directory = await Path.home() / 'AppData' / 'Local' / 'linux_recognition'
    elif system_used == 'Linux':
        data_directory = await Path.home() / '.local' / 'share' / 'linux_recognition'
    else:
        project_directory = await get_project_directory()
        data_directory = project_directory.parent  / '.linux_recognition'
    return data_directory
