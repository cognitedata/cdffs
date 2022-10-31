import pytest
import responses
from cognite.client import ClientConfig, global_config
from cognite.client.credentials import APIKey

from cognite.cdffs.spec import CdfFileSystem

global_config.disable_pypi_version_check = True


@pytest.mark.usefixtures("mock_cognite_client")
@pytest.fixture(scope="class")
def fs():
    inp = {
        "connection_config": ClientConfig(
            client_name="foobar",
            base_url="https://foobar.cognitedata.com",
            project="foobar",
            credentials=APIKey("dummy-secret"),
        )
    }
    fs = CdfFileSystem(**inp)
    return fs


@pytest.fixture
def mock_files_response():
    with responses.RequestsMock() as response:
        response_body = {
            "items": [
                {
                    "externalId": "test.zarr/.zmetadata",
                    "name": "string",
                    "directory": "/sample_data/out/",
                },
                {
                    "externalId": "test.zarr/.zattrs",
                    "name": "string",
                    "directory": "/sample_data/out/",
                },
                {
                    "externalId": "test.zarr/.zgroup",
                    "name": "string",
                    "directory": "/sample_data/out/",
                },
                {
                    "externalId": "test.zarr/x/.zarray",
                    "name": "string",
                    "directory": "/sample_data/out/",
                },
                {
                    "externalId": "test.zarr/x/.zgroup",
                    "name": "string",
                    "directory": "/sample_data/out/",
                },
                {
                    "externalId": "test.zarr/x/0",
                    "name": "string",
                    "directory": "/sample_data/out/",
                },
                {
                    "externalId": "test.zarr/y/.zarray",
                    "name": "string",
                    "directory": "/sample_data/out/",
                },
                {
                    "externalId": "test.zarr/y/.zgroup",
                    "name": "string",
                    "directory": "/sample_data/out/",
                },
                {
                    "externalId": "test.zarr/y/0",
                    "name": "string",
                    "directory": "/sample_data/out/",
                },
            ]
        }

        url_pattern = "https://foobar.cognitedata.com/api/v1/projects/foobar/files/list"
        response.assert_all_requests_are_fired = False

        response.add(response.POST, url_pattern, status=200, json=response_body)
        response.add(response.GET, url_pattern, status=200, json=response_body)
        yield response


@pytest.fixture
def base(request) -> int:
    return request.param


@pytest.mark.parametrize(
    "test_input,expected_result",
    [
        (
            "cdffs://sample_data/test/.zgroup",
            "sample_data/test/.zgroup",
        ),
        (
            "cdffs1://sample_data/test/.zgroup",
            "cdffs1://sample_data/test/.zgroup",
        ),
    ],
)
def test_strip_protocol(fs, test_input, expected_result):
    fs.metadata = {}
    result_data = fs._strip_protocol(test_input)
    assert expected_result == result_data


@pytest.mark.parametrize(
    "test_input,expected_result",
    [
        (
            "/sample_data/test.zarr/1/.zgroup",
            ("/sample_data", "test.zarr", "test.zarr/1/.zgroup"),
        ),
        (
            "/sample_data/test.zarr/1",
            ("/sample_data", "test.zarr", "test.zarr/1"),
        ),
        (
            "/sample_data/test.zarr/lat/58.91",
            ("/sample_data", "test.zarr", "test.zarr/lat/58.91"),
        ),
        (
            "sample_data/test.zarr/lat/58.91",
            ("/sample_data", "test.zarr", "test.zarr/lat/58.91"),
        ),
        (
            "/sample_data/test.unk/out/test/21.dat",
            ("/sample_data", "test.unk", "test.unk/out/test/21.dat"),
        ),
        (
            "/sample_data/child_dir/info/test.unk/out/test/21.dat",
            ("/sample_data/child_dir/info", "test.unk", "test.unk/out/test/21.dat"),
        ),
        (
            "/sample_data/child_dir/info/test/out/test/sa.json",
            ("/sample_data/child_dir/info/test/out/test", "sa.json", "sa.json"),
        ),
        (
            "/sample_data/21.geojson",
            ("/sample_data", "21.geojson", "21.geojson"),
        ),
        (
            "/sample_data/test/sa.geojson",
            ("/sample_data/test", "sa.geojson", "sa.geojson"),
        ),
        (
            "sa.txt",
            ("/", "sa.txt", "sa.txt"),
        ),
    ],
)
def test_split_path(fs, test_input, expected_result):
    fs.metadata = {}
    result_data = fs.split_path(test_input)
    assert expected_result == result_data


@pytest.mark.parametrize(
    "test_input,expected_result",
    [
        (
            "/sample_data/test.zarr/1/.zgroup",
            ("/sample_data", "test.zarr", "test.zarr/1/.zgroup"),
        ),
        (
            "/sample_data/test.zarr/1",
            ("/sample_data", "test.zarr", "test.zarr/1"),
        ),
        (
            "/sample_data/test.zarr/lat/58.91",
            ("/sample_data", "test.zarr", "test.zarr/lat/58.91"),
        ),
        (
            "/sample_data/test.unk/out/test/21.dat",
            ("/sample_data", "test.unk", "test.unk/out/test/21.dat"),
        ),
        (
            "sample_data/test.zarr/lat/58.91",
            ("/sample_data", "test.zarr", "test.zarr/lat/58.91"),
        ),
    ],
)
def test_split_path_with_directory(fs, test_input, expected_result):
    fs.metadata = {"directory": "/sample_data"}
    result_data = fs.split_path(test_input)

    assert expected_result == result_data


@pytest.mark.parametrize(
    "test_input,expected_result",
    [
        (
            "/sample_data/child_dir/info/test.unk/out/test/21.dat",
            ("/sample_data/child_dir/info", "test.unk", "test.unk/out/test/21.dat"),
        ),
    ],
)
def test_split_path_with_directory_extended(fs, test_input, expected_result):
    fs.metadata = {"directory": "/sample_data/child_dir/info/"}
    result_data = fs.split_path(test_input)
    assert expected_result == result_data


@pytest.mark.parametrize(
    "validate_suffix, test_input,expected_result",
    [
        (False, "/sample_data", ("/sample_data", "", "")),
        (False, "/sample_data/test/", ("/sample_data/test", "", "")),
        (False, "/sample_data/test/out/21", ("/sample_data/test/out/21", "", "")),
    ],
)
def test_split_path_validate_suffix(fs, validate_suffix, test_input, expected_result):
    fs.metadata = {}
    result_data = fs.split_path(test_input, validate_suffix=validate_suffix)
    assert expected_result == result_data


@pytest.mark.parametrize(
    "test_input",
    [
        ("/sample_data"),
        ("/sample_data/test/"),
        ("/sample_data/test/out/21"),
    ],
)
def test_split_path_exception(fs, test_input):
    fs.metadata = {}
    with pytest.raises(ValueError):
        fs.split_path(test_input)


@pytest.mark.parametrize(
    "detail_flag, test_input, expected_result",
    [
        (
            False,
            "sample_data/out/test.zarr",
            [
                "sample_data/out/test.zarr",
                "sample_data/out/test.zarr/.zmetadata",
                "sample_data/out/test.zarr/.zattrs",
                "sample_data/out/test.zarr/.zgroup",
                "sample_data/out/test.zarr/x",
                "sample_data/out/test.zarr/y",
            ],
        ),
        (
            False,
            "sample_data/out/test.zarr/x",
            [
                "sample_data/out/test.zarr/x",
                "sample_data/out/test.zarr/x/.zarray",
                "sample_data/out/test.zarr/x/.zgroup",
                "sample_data/out/test.zarr/x/0",
            ],
        ),
        (
            True,
            "sample_data/out/test.zarr",
            [
                {"type": "directory", "name": "sample_data/out/test.zarr"},
                {"type": "file", "name": "sample_data/out/test.zarr/.zmetadata", "size": -1},
                {"type": "file", "name": "sample_data/out/test.zarr/.zattrs", "size": -1},
                {"type": "file", "name": "sample_data/out/test.zarr/.zgroup", "size": -1},
                {"type": "directory", "name": "sample_data/out/test.zarr/x"},
                {"type": "directory", "name": "sample_data/out/test.zarr/y"},
            ],
        ),
        (
            True,
            "sample_data/out/test.zarr/x",
            [
                {"type": "directory", "name": "sample_data/out/test.zarr/x"},
                {"type": "file", "name": "sample_data/out/test.zarr/x/.zarray", "size": -1},
                {"type": "file", "name": "sample_data/out/test.zarr/x/.zgroup", "size": -1},
                {"type": "file", "name": "sample_data/out/test.zarr/x/0", "size": -1},
            ],
        ),
    ],
)
def test_ls(fs, detail_flag, test_input, expected_result, mock_files_response):
    fs.metadata = {}
    result_data = fs.ls(test_input, detail=detail_flag)
    print(expected_result)
    print(result_data)
    assert expected_result == result_data
