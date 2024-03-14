"""Construct cognite client config from environment variables."""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials, Token

from vendor.pydantic.class_validators import validator
from vendor.pydantic.env_settings import BaseSettings
from vendor.pydantic.types import SecretStr


def validate_scopes(cls: Any, value: str) -> Optional[List]:
    """Validate scopes and reformat them to a list."""
    if value:
        return value.split(",")
    return None


class FsConfig(BaseSettings):
    """Base config to parse environment variables."""

    class Config:
        """Global config for Base Settings."""

        env_nested_delimiter = "__"
        env_file = ".env"
        env_file_encoding = "utf-8"

    # model_config: ConfigDict = ConfigDict(env_nested_delimiter="__", env_file=".env", env_file_encoding="utf-8")


class FsCredentials(FsConfig, ABC):
    """Credentials.

    Attributes:
        cognite_project (str): Cdf Project name.
        cdf_cluster (str): Cdf Cluster URL.
    """

    cognite_project: Optional[str] = None
    cdf_cluster: Optional[str] = None

    @abstractmethod
    def get_credentials(self) -> Any:
        """Get credentials to create a client config."""

    def get_client_config(self) -> ClientConfig:
        """Get Cognite client config.

        Returns:
            ClientConfig: Cognite Client Config.
        """
        # Create credentials.
        creds = self.get_credentials()

        client_cnf = ClientConfig(
            client_name="cognite-client",
            base_url=f"https://{self.cdf_cluster}.cognitedata.com",
            project=self.cognite_project,
            credentials=creds,
        )
        return client_cnf


class FsOAuthCredentials(FsCredentials, FsConfig):
    """OAuth Credentials for cdffs.

    Attributes:
        token_url (str): Token url to fetch access tokens.
        client_id (str): Client Id.
        client_secret (SecretStr): Client Secret.
        scopes (str): List of scopes as comma(,) separated string.
    """

    token_url: Optional[str] = None
    client_id: Optional[SecretStr] = None
    client_secret: Optional[SecretStr] = None
    scopes: Optional[Union[str, List]] = None

    # Validator
    _scopes = validator("scopes")(validate_scopes)

    def get_credentials(self) -> OAuthClientCredentials:
        """Construct credentials based on environment variables.

        Returns:
            OAuthClientCredentials: Credentials to create a Cognite Client Config.
        """
        # Create credentials to create CDF Client Config.
        creds = OAuthClientCredentials(
            token_url=self.token_url,
            client_id=self.client_id.get_secret_value() if self.client_id else None,
            client_secret=self.client_secret.get_secret_value() if self.client_secret else None,
            scopes=self.scopes,
        )
        return creds


class FsToken(FsCredentials, FsConfig):
    """Token Credential.

    Attributes:
        token (SecretStr): Token.
    """

    token: Optional[SecretStr] = None

    def get_credentials(self) -> Token:
        """Construct token credential based on environment variable.

        Returns:
            Token: Token credential to create a Cognite Client Config.
        """
        # Create credentials to create CDF Client Config.
        creds = Token(self.token.get_secret_value() if self.token else None)
        return creds


def get_connection_config(env_file: str) -> CogniteClient:
    """Construct Cognite Client from environment variables."""
    credentials = FsOAuthCredentials(_env_file=env_file)  # type:ignore
    connection_config = None
    if all(value is not None for _, value in credentials.dict().items()):
        connection_config = credentials.get_client_config()
    else:
        token = FsToken(_env_file=env_file)  # type:ignore
        if all(value is not None for _, value in token.dict().items()):
            connection_config = token.get_client_config()

    return connection_config
