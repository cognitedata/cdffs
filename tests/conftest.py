from unittest import mock

import pytest
from cognite.client import CogniteClient


@pytest.fixture
def mock_cognite_client():
    with mock.patch("CogniteClient") as client_mock:
        cog_client_mock = mock.MagicMock(spec=CogniteClient)
        client_mock.return_value = cog_client_mock
        yield
