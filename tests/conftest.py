import os
from unittest import mock

import pytest
from cognite.client import CogniteClient


@pytest.fixture
def mock_cognite_client():
    with mock.patch("CogniteClient") as client_mock:
        cog_client_mock = mock.MagicMock(spec=CogniteClient)
        client_mock.return_value = cog_client_mock
        yield


@pytest.fixture
def mock_set_oauth_credentials():
    with mock.patch.dict(
        os.environ,
        {
            "TOKEN_URL": "https://foobar/oauth2/token",
            "CLIENT_ID": "a5aaa16b-ccca-461f-b55d-91af56aa84de",
            "CLIENT_SECRET": "a5aaa16b-ccca-461f-b55d-91af56aa84de",
            "COGNITE_PROJECT": "foobar",
            "CDF_CLUSTER": "foobar",
            "SCOPES": "https://foobar.cognitedata.com/.default",
        },
        clear=True,
    ):
        yield


@pytest.fixture
def mock_set_token():
    with mock.patch.dict(
        os.environ,
        {
            "TOKEN": "54129bff-fdd8-498a-9edd-b0538ba5248454129bff-fdd8-498a-9edd-b0538ba5248454129bff-fdd8",
            "COGNITE_PROJECT": "foobar",
            "CDF_CLUSTER": "foobar",
        },
        clear=True,
    ):
        yield


@pytest.fixture
def mock_unset_env():
    with mock.patch.dict(os.environ, {}, clear=True):
        yield
