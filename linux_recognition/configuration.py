import logging
from contextlib import contextmanager
from typing import Self, NamedTuple

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
    port: str

    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)

    # noinspection PyNestedDecorators
    @field_validator('port', mode='before')
    @classmethod
    def ensure_range(cls, value: int) -> str:
        if not isinstance(value, int) or not 1 <= value <= 65535:
            raise ValueError('The value must be an integer between 1 and 65535.')
        return str(value)

    def for_database(self, database_name: str) -> Self:
        return self.model_copy(update={'dbname': database_name})


class CoreDatabases(NamedTuple):
    recognized: str
    packages: str
    repology: str


class DatabaseSettings(BaseModel):
    core_databases: CoreDatabases
    postgres_default: PostgresConfig
    postgres_udd: PostgresConfig
    psql_directory: str | None = None

    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)

    # noinspection PyNestedDecorators
    @field_validator('psql_directory', mode='after')
    @classmethod
    def get_path(cls, directory: str) -> Path | None:
        return Path(directory) if directory is not None else None


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


@contextmanager
def validated_credentials(env_file: Path, env_prefix: str):

    class Credentials(BaseSettings):
        pypi_user_agent: SecretStr = Field(exclude=True)
        sourceforge_bearer: SecretStr = Field(exclude=True)
        obs_username: SecretStr = Field(exclude=True)
        obs_password: SecretStr = Field(exclude=True)
        github_token: SecretStr = Field(exclude=True)
        openai_api_key: SecretStr = Field(exclude=True)

        model_config = SettingsConfigDict(
            env_file=env_file,
            env_prefix=env_prefix,
            extra='ignore',
            hide_input_in_errors=True
        )

    # noinspection PyArgumentList
    Credentials()
    yield


class Settings(BaseSettings):
    database: DatabaseSettings
    logging: LoggingSettings
    openai: OpenAiModels


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
        # noinspection PyArgumentList
        return SettingsClass()


async def get_project_directory() -> Path:
    file_path = await Path(__file__).resolve()
    return file_path.parent
