import os
from unittest import mock

import pytest
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import Token

from cognite.cdffs import CdfFileSystem


@pytest.fixture
def mock_cognite_client():
    with mock.patch("CogniteClient") as client_mock:
        cog_client_mock = mock.MagicMock(spec=CogniteClient)
        client_mock.return_value = cog_client_mock
        yield


@pytest.fixture(scope="function", autouse=True)
def mock_unset_env():
    with mock.patch.dict(os.environ, values={}, clear=True):
        yield


@pytest.mark.usefixtures("mock_cognite_client")
@pytest.fixture(scope="function")
def fs():
    inp = {
        "connection_config": ClientConfig(
            client_name="foobar",
            base_url="https://foobar.cognitedata.com",
            project="foobar",
            credentials=Token("dummy-token"),
        )
    }
    fs = CdfFileSystem(**inp)
    return fs


@pytest.mark.usefixtures("mock_cognite_client")
@pytest.fixture(scope="function")
def az_fs():
    inp = {
        "connection_config": ClientConfig(
            client_name="foobar",
            base_url="https://foobar.cognitedata.com",
            project="foobar",
            credentials=Token("dummy-token"),
        ),
        "upload_strategy": "azure",
    }
    fs = CdfFileSystem(**inp)
    return fs


@pytest.mark.usefixtures("mock_cognite_client")
@pytest.fixture(scope="function")
def gcp_fs():
    inp = {
        "connection_config": ClientConfig(
            client_name="foobar",
            base_url="https://foobar.cognitedata.com",
            project="foobar",
            credentials=Token("dummy-token"),
        ),
        "upload_strategy": "google",
    }
    fs = CdfFileSystem(**inp)
    return fs
