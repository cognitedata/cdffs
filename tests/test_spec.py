# type: ignore
# pylint: disable=missing-function-docstring
import os

import pytest
import responses
from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import OAuthClientCredentials, Token
from cognite.client.data_classes.files import FileMetadata

from cognite.cdffs.spec import CdfFileSystem

global_config.disable_pypi_version_check = True


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


@pytest.fixture
def mock_files_ls_response(request):
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
        if request.param == "successful":
            response.add(response.POST, url_pattern, status=200, json=response_body)
        elif request.param == "successful-no-files":
            response.add(response.POST, url_pattern, status=200, json={"items": []})
        elif request.param == "failure":
            response.add(
                response.POST,
                url_pattern,
                status=400,
                json={"error": {"code": 401, "message": "Could not authenticate."}},
            )
        yield response


@pytest.fixture
def mock_files_ls_error_response():
    with responses.RequestsMock() as response:
        response_body = {
            "error": {"code": 401, "message": "Could not authenticate.", "missing": [{}], "duplicated": [{}]}
        }

        url_pattern = "https://foobar.cognitedata.com/api/v1/projects/foobar/files/list"
        response.assert_all_requests_are_fired = False

        response.add(response.POST, url_pattern, status=400, json=response_body)
        yield response


@pytest.fixture
def mock_files_upload_response(request):
    with responses.RequestsMock() as response:
        upload_response_body = {
            "externalId": "df.csv",
            "name": "df.csv",
            "directory": "/sample_data/out/sample/",
            "source": "test",
            "mimeType": "text/plain",
            "metadata": {"size": 100},
            "dataSetId": 783443182507908,
            "id": 783443232507908,
            "uploaded": True,
            "uploadedTime": 1667464449,
            "labels": [],
            "createdTime": 1667464449,
            "lastUpdatedTime": 1667464449,
            "uploadUrl": "https://azure.blob-test.com/uploaddata",
        }

        wirte_url_pattern = "https://foobar.cognitedata.com/api/v1/projects/foobar/files"
        azure_upload_url_pattern = "https://azure.blob-test.com/uploaddata"
        response.assert_all_requests_are_fired = False

        if request.param == "successful":
            response.add(response.POST, wirte_url_pattern, status=200, json=upload_response_body)
            response.add(response.PUT, azure_upload_url_pattern, status=200, json={})
        else:
            response.add(
                response.POST,
                wirte_url_pattern,
                status=500,
                json={},
            )
        yield response


@pytest.fixture
def mock_files_download_response(request):
    with responses.RequestsMock() as response:
        list_response_body = {
            "items": [
                {
                    "externalId": "df.csv",
                    "name": "df.csv",
                    "directory": "/sample_data/out/sample/",
                    "source": "test",
                    "mimeType": "text/plain",
                    "metadata": {"size": 100},
                    "dataSetId": 783443182507908,
                    "labels": [],
                    "id": 783443232507908,
                    "uploaded": True,
                    "uploadedTime": 1667464449,
                    "createdTime": 1667464449,
                    "lastUpdatedTime": 1667464449,
                }
            ],
        }

        list_url_pattern = "https://foobar.cognitedata.com/api/v1/projects/foobar/files/list"
        read_url_pattern = "https://foobar.cognitedata.com/api/v1/projects/foobar/files/downloadlink"
        azure_download_url_pattern = "https://azure.blob-test.com/downloaddata"
        response.assert_all_requests_are_fired = False

        if request.param == "successful":
            download_url_response_body = {
                "items": [
                    {
                        "downloadUrl": "https://azure.blob-test.com/downloaddata",
                        "externalId": "df.csv",
                    }
                ]
            }

            download_response_body = ",A,B\n0,1,2\n1,4,5\n".encode("utf-8")
            response.add(response.POST, list_url_pattern, status=200, json=list_response_body)
            response.add(response.POST, read_url_pattern, status=200, json=download_url_response_body)
            response.add(response.GET, azure_download_url_pattern, status=200, body=download_response_body)
        elif request.param == "failure-file-not-ready":
            download_url_response_body = {"error": {"code": 401, "message": "Files not uploaded: 783443232507908"}}
            response.add(response.POST, list_url_pattern, status=200, json=list_response_body)
            response.add(response.POST, read_url_pattern, status=400, json=download_url_response_body)
        elif request.param == "failure-file-missing":
            download_url_response_body = {
                "error": {"code": 401, "message": "Files missing", "missing": {"externalId": "df.csv"}}
            }
            response.add(response.POST, list_url_pattern, status=200, json=list_response_body)
            response.add(response.POST, read_url_pattern, status=400, json=download_url_response_body)
        yield response


@pytest.fixture
def mock_files_delete_response(request):
    with responses.RequestsMock() as response:
        delete_url_pattern = "https://foobar.cognitedata.com/api/v1/projects/foobar/files/delete"
        response.assert_all_requests_are_fired = False

        if request.param == "successful":
            response.add(response.POST, delete_url_pattern, status=200, json={})
        else:
            json_response = {"error": {"code": 400, "message": "not found", "missing": [{"id": 1}]}}
            response.add(response.POST, delete_url_pattern, status=400, json=json_response)

        yield response


@pytest.fixture
def mock_files_byids_response(request):
    with responses.RequestsMock() as response:
        byids_url_pattern = "https://foobar.cognitedata.com/api/v1/projects/foobar/files/byids"
        response.assert_all_requests_are_fired = False

        if request.param == "successful":
            response.add(
                response.POST,
                byids_url_pattern,
                status=200,
                json={
                    "items": [
                        {
                            "externalId": "df.csv",
                            "name": "df.csv",
                            "directory": "/sample_data/out/sample/",
                        },
                    ]
                },
            )
        else:
            json_response = {"error": {"code": 400, "message": "not found", "missing": [{"id": 1}]}}
            response.add(response.POST, byids_url_pattern, status=400, json=json_response)

        yield response


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
    fs.file_metadata = FileMetadata(metadata={})
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
    fs.file_metadata = FileMetadata(metadata={})
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
    fs.file_metadata = FileMetadata(directory="/sample_data")
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
    fs.file_metadata = FileMetadata(directory="/sample_data/child_dir/info/")
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
    fs.file_metadata = FileMetadata(metadata={})
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
    fs.file_metadata = FileMetadata(metadata={})
    with pytest.raises(ValueError):
        fs.split_path(test_input)


@pytest.mark.parametrize(
    "detail_flag, test_input, expected_result, mock_files_ls_response",
    [
        (
            False,
            "sample_data/out/test.zarr",
            [
                "sample_data/out/test.zarr/.zmetadata",
                "sample_data/out/test.zarr/.zattrs",
                "sample_data/out/test.zarr/.zgroup",
                "sample_data/out/test.zarr/x",
                "sample_data/out/test.zarr/y",
                "sample_data/out/test.zarr",
            ],
            "successful",
        ),
        (
            False,
            "sample_data/out/test.zarr/x",
            [
                "sample_data/out/test.zarr/x/.zarray",
                "sample_data/out/test.zarr/x/.zgroup",
                "sample_data/out/test.zarr/x/0",
                "sample_data/out/test.zarr/x",
            ],
            "successful",
        ),
        (
            False,
            "/sample_data/out/test.zarr/x",
            [
                "sample_data/out/test.zarr/x/.zarray",
                "sample_data/out/test.zarr/x/.zgroup",
                "sample_data/out/test.zarr/x/0",
                "sample_data/out/test.zarr/x",
            ],
            "successful",
        ),
        (
            True,
            "sample_data/out/test.zarr",
            [
                {"type": "file", "name": "sample_data/out/test.zarr/.zmetadata", "size": -1},
                {"type": "file", "name": "sample_data/out/test.zarr/.zattrs", "size": -1},
                {"type": "file", "name": "sample_data/out/test.zarr/.zgroup", "size": -1},
                {"type": "directory", "name": "sample_data/out/test.zarr/x"},
                {"type": "directory", "name": "sample_data/out/test.zarr/y"},
                {"type": "directory", "name": "sample_data/out/test.zarr"},
            ],
            "successful",
        ),
        (
            True,
            "sample_data/out/test.zarr/x",
            [
                {"type": "file", "name": "sample_data/out/test.zarr/x/.zarray", "size": -1},
                {"type": "file", "name": "sample_data/out/test.zarr/x/.zgroup", "size": -1},
                {"type": "file", "name": "sample_data/out/test.zarr/x/0", "size": -1},
                {"type": "directory", "name": "sample_data/out/test.zarr/x"},
            ],
            "successful",
        ),
        (
            True,
            "/sample_data/out/test.zarr/x",
            [
                {"type": "file", "name": "sample_data/out/test.zarr/x/.zarray", "size": -1},
                {"type": "file", "name": "sample_data/out/test.zarr/x/.zgroup", "size": -1},
                {"type": "file", "name": "sample_data/out/test.zarr/x/0", "size": -1},
                {"type": "directory", "name": "sample_data/out/test.zarr/x"},
            ],
            "successful",
        ),
    ],
    indirect=["mock_files_ls_response"],
)
@pytest.mark.usefixtures("mock_files_ls_response")
def test_ls(fs, detail_flag, test_input, expected_result):
    fs.file_metadata = FileMetadata(metadata={})
    result_data = fs.ls(test_input, detail=detail_flag)
    assert expected_result == result_data


@pytest.mark.parametrize(
    "test_input, expected_result",
    [
        (
            "sample_data/out_mkdir/",
            ([{"type": "directory", "name": "sample_data/out_mkdir/"}]),
        ),
    ],
)
def test_mkdir(fs, test_input, expected_result):
    fs.file_metadata = FileMetadata(metadata={})
    fs.mkdir(test_input)
    assert fs.dircache[test_input] == expected_result

    # It should not ignore the new directory.
    with pytest.raises(FileExistsError):
        fs.mkdir(test_input)


@pytest.mark.parametrize(
    "test_input, expected_result",
    [
        (
            "sample_data/out_makedir/",
            ([{"type": "directory", "name": "sample_data/out_makedir/"}]),
        ),
    ],
)
def test_makedir(fs, test_input, expected_result):
    fs.file_metadata = FileMetadata(metadata={})
    fs.makedir(test_input)
    assert fs.dircache[test_input] == expected_result

    # It should ignore the new directory.
    fs.makedir(test_input)
    assert fs.dircache[test_input] == expected_result


@pytest.mark.parametrize(
    "exist_ok_flag, test_input, expected_result",
    [
        (
            False,
            "sample_data/test_output/",
            ([{"type": "directory", "name": "sample_data/test_output/"}]),
        ),
    ],
)
def test_makedirs_exception(fs, exist_ok_flag, test_input, expected_result):
    fs.file_metadata = FileMetadata(metadata={})
    fs.makedirs(test_input, exist_ok=exist_ok_flag)
    assert fs.dircache[test_input] == expected_result

    # It should raise an exception as the new directory already exists.
    with pytest.raises(FileExistsError):
        fs.makedirs(test_input, exist_ok=exist_ok_flag)


@pytest.mark.parametrize(
    "test_path, test_mode, test_data, expected_len, mock_files_upload_response",
    [
        (
            "/sample_data/out/sample/df.csv",
            "wb",
            ",A,B\n0,1,2\n1,4,5\n",
            17,
            "successful",
        )
    ],
    indirect=["mock_files_upload_response"],
)
@pytest.mark.usefixtures("mock_files_upload_response")
def test_open_write(fs, test_path, test_mode, test_data, expected_len):
    fs.file_metadata = FileMetadata(metadata={})
    cdf_file = fs.open(test_path, mode=test_mode)
    test_len = cdf_file.write(test_data.encode("utf-8"))
    assert test_len == expected_len


@pytest.mark.parametrize(
    "test_path, test_out_length, expected_result, cache_type, mock_files_download_response",
    [
        (
            "sample_data/out/sample/df.csv",
            -1,
            ",A,B\n0,1,2\n1,4,5\n".encode("utf-8"),
            "readahead",
            "successful",
        ),
        (
            "sample_data/out/sample/df.csv",
            11,
            ",A,B\n0,1,2\n".encode("utf-8"),
            "bytes",
            "successful",
        ),
        (
            "sample_data/out/sample/df.csv",
            -1,
            ",A,B\n0,1,2\n1,4,5\n".encode("utf-8"),
            "all",
            "successful",
        ),
    ],
    indirect=["mock_files_download_response"],
)
@pytest.mark.usefixtures("mock_files_download_response")
def test_open_read(fs, test_path, test_out_length, expected_result, cache_type):
    fs.file_metadata = FileMetadata(metadata={})
    cdf_file = fs.open(test_path, mode="rb", cache_type=cache_type)
    test_result = cdf_file.read(length=test_out_length)
    assert test_result == expected_result


@pytest.mark.parametrize(
    "test_path, test_recursive, expected_result, mock_files_download_response",
    [
        (
            "sample_data/out/sample/df.csv",
            False,
            ",A,B\n0,1,2\n1,4,5\n".encode("utf-8"),
            "successful",
        ),
        (
            ["sample_data/out/sample/df.csv"],
            True,
            {
                "sample_data/out/sample/df.csv": ",A,B\n0,1,2\n1,4,5\n".encode("utf-8"),
            },
            "successful",
        ),
    ],
    indirect=["mock_files_download_response"],
)
@pytest.mark.usefixtures("mock_files_download_response")
def test_cat(fs, test_path, test_recursive, expected_result):
    fs.file_metadata = FileMetadata(metadata={})
    test_result = fs.cat(test_path, recursive=test_recursive)
    assert test_result == expected_result


@pytest.mark.parametrize(
    "test_path, mock_files_delete_response",
    [
        (
            "sample_data/out/sample/df.csv",
            "successful",
        ),
        (
            "sample_data/out/sample/",
            "successful",
        ),
        (
            "sample_data/out/sample/",  # Directory delete will be successful always
            "failed",
        ),
    ],
    indirect=["mock_files_delete_response"],
)
@pytest.mark.usefixtures("mock_files_delete_response")
def test_rm(fs, test_path):
    fs.rm(test_path)


@pytest.mark.parametrize(
    "test_path, mock_files_delete_response",
    [
        (
            ["sample_data/out/sample/df.csv", "sample_data/out/sample/df2.csv", "sample_data/out/sample/df3.csv"],
            "successful",
        ),
        (
            ["sample_data/out/sample1/", "sample_data/out/sample2/", "sample_data/out/sample3/"],
            "successful",
        ),
        (
            ["sample_data/out/sample/", "sample_data/out/sample2/"],  # Directory delete will be successful always
            "failed",
        ),
    ],
    indirect=["mock_files_delete_response"],
)
@pytest.mark.usefixtures("mock_files_delete_response")
def test_rm_files(fs, test_path):
    fs.rm_files(test_path)


@pytest.mark.parametrize(
    "test_path, mock_files_byids_response",
    [
        (
            "sample_data/out/sample/df.csv",
            "successful",
        ),
        (
            "sample_data/out/sample/df.csv",
            "failed",
        ),
        (
            "sample_data/out/sample/",  # Directory delete will be failed.
            "successful",
        ),
    ],
    indirect=["mock_files_byids_response"],
)
@pytest.mark.usefixtures("mock_files_byids_response")
def test_exists(fs, test_path):
    fs.exists(test_path)


@pytest.fixture(scope="function")
def oauth_fs(monkeypatch):
    monkeypatch.setitem(os.environ, "TOKEN_URL", "https://foobar/oauth2/token")
    monkeypatch.setitem(os.environ, "CLIENT_ID", "a5aaa16b-ccca-461f-b55d-91af56aa84de")
    monkeypatch.setitem(os.environ, "CLIENT_SECRET", "a5aaa16b-ccca-461f-b55d-91af56aa84de")
    monkeypatch.setitem(os.environ, "COGNITE_PROJECT", "foobar")
    monkeypatch.setitem(os.environ, "CDF_CLUSTER", "foobar")
    monkeypatch.setitem(os.environ, "SCOPES", "https://foobar.cognitedata.com/.default")
    return CdfFileSystem("dummy-oauth-connection-config")


def test_oauth_credentials(oauth_fs):
    assert isinstance(oauth_fs.cognite_client, CogniteClient)
    assert isinstance(oauth_fs.cognite_client.config.credentials, OAuthClientCredentials)


@pytest.fixture(scope="function")
def token_fs(monkeypatch):
    monkeypatch.setitem(
        os.environ, "TOKEN", "54129bff-fdd8-498a-9edd-b0538ba5248454129bff-fdd8-498a-9edd-b0538ba5248454129bff-fdd8"
    )
    monkeypatch.setitem(os.environ, "COGNITE_PROJECT", "foobar")
    monkeypatch.setitem(os.environ, "CDF_CLUSTER", "foobar")
    return CdfFileSystem("dummy-token-connection-config")


def test_token(token_fs):
    assert isinstance(token_fs.cognite_client, CogniteClient)
    assert isinstance(token_fs.cognite_client.config.credentials, Token)


@pytest.fixture(scope="function")
def unset_fs(monkeypatch):
    monkeypatch.setitem(os.environ, "TOKEN", "")
    return CdfFileSystem("dummy-unset-connection-config")


# test exception scenarios
@pytest.mark.parametrize("connection_config", [None, "test"])
def test_do_connect_with_inv_args(connection_config):
    with pytest.raises(ValueError):
        CdfFileSystem(connection_config)


def test_initialize_cdf_file_system_with_inv_args():
    inp = {
        "connection_config": ClientConfig(
            client_name="foobar-foobar",
            base_url="https://foobar-foobar.cognitedata.com",
            project="foobar-foobar",
            credentials=Token("dummy-token"),
            timeout=1,
        )
    }
    with pytest.raises(ValueError):
        CdfFileSystem(inp, file_metadata="invalid-string")


@pytest.mark.parametrize(
    "test_input, mock_files_ls_response",
    [
        (
            "sample_data/out/test.zarr",
            "successful-no-files",
        ),
    ],
    indirect=["mock_files_ls_response"],
)
@pytest.mark.usefixtures("mock_files_ls_response")
def test_ls_no_files_exception(fs, test_input):
    fs.file_metadata = FileMetadata(metadata={})
    with pytest.raises(FileNotFoundError):
        fs.ls(test_input)


@pytest.mark.parametrize(
    "test_input, mock_files_ls_response",
    [
        (
            "sample_data/out/test.zarr",
            "failure",
        ),
    ],
    indirect=["mock_files_ls_response"],
)
@pytest.mark.usefixtures("mock_files_ls_response")
def test_ls_cognite_exception(fs, test_input):
    fs.file_metadata = FileMetadata(metadata={})
    with pytest.raises(FileNotFoundError):
        fs.ls(test_input)


@pytest.mark.parametrize(
    "test_path, test_out_length, mock_files_download_response",
    [
        (
            "sample_data/out/sample/df.csv",
            -1,
            "failure-file-not-ready",
        ),
        (
            "sample_data/out/sample/df.csv",
            -1,
            "failure-file-missing",
        ),
    ],
    indirect=["mock_files_download_response"],
)
@pytest.mark.usefixtures("mock_files_download_response")
def test_open_read_exception(fs, test_path, test_out_length):
    fs.file_metadata = FileMetadata(metadata={})
    with pytest.raises(FileNotFoundError):
        cdf_file = fs.open(test_path, mode="rb")
        cdf_file.read(length=test_out_length)


def test_cat_empty_path_exception(fs):
    with pytest.raises(ValueError):
        fs.cat("")


@pytest.mark.parametrize(
    "test_path, mock_files_delete_response",
    [
        (
            "sample_data/out/sample/df.csv",
            "failed",
        )
    ],
    indirect=["mock_files_delete_response"],
)
@pytest.mark.usefixtures("mock_files_delete_response")
def test_rm_exception(fs, test_path):
    with pytest.raises(FileNotFoundError):
        fs.rm(test_path)


def test_cd_exception(fs):
    with pytest.raises(NotImplementedError):
        fs.cd("/test/")


def test_mv_exception(fs):
    with pytest.raises(NotImplementedError):
        fs.mv("/test/", "/test2/")


@pytest.mark.parametrize(
    "test_path, test_out_length, mock_files_download_response",
    [
        (
            "sample_data/out/sample/df.csv",
            -1,
            "failure-file-not-ready",
        ),
        (
            "sample_data/out/sample/df.csv",
            -1,
            "failure-file-missing",
        ),
    ],
    indirect=["mock_files_download_response"],
)
@pytest.mark.usefixtures("mock_files_download_response")
def test_open_read_exception_without_retry(test_path, test_out_length):
    inp = {
        "connection_config": ClientConfig(
            client_name="foobar",
            base_url="https://foobar.cognitedata.com",
            project="foobar",
            credentials=Token("dummy-token"),
        ),
        "download_retries": False,
    }
    fs = CdfFileSystem(**inp)
    fs.file_metadata = FileMetadata(metadata={})
    with pytest.raises(FileNotFoundError):
        cdf_file = fs.open(test_path, mode="rb")
        cdf_file.read(length=test_out_length)


@pytest.mark.parametrize(
    "test_path, test_out_length, mock_files_download_response",
    [
        (
            "sample_data/out/sample/df.csv",
            -1,
            "failure-file-not-ready",
        ),
        (
            "sample_data/out/sample/df.csv",
            -1,
            "failure-file-missing",
        ),
    ],
    indirect=["mock_files_download_response"],
)
@pytest.mark.usefixtures("mock_files_download_response")
def test_open_read_exception_with_retry(test_path, test_out_length):
    inp = {
        "connection_config": ClientConfig(
            client_name="foobar",
            base_url="https://foobar.cognitedata.com",
            project="foobar",
            credentials=Token("dummy-token"),
        ),
        "download_retries": True,
        "max_download_retries": 2,
    }
    fs = CdfFileSystem(**inp)
    fs.file_metadata = FileMetadata(metadata={})
    with pytest.raises(FileNotFoundError):
        cdf_file = fs.open(test_path, mode="rb")
        cdf_file.read(length=test_out_length)
