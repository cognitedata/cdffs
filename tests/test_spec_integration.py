# type: ignore
# pylint: disable=missing-function-docstring

import json
import os
import time

import dask.dataframe as dd
import geodatasets
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.files import FileMetadata

from cognite.cdffs.spec import CdfFileSystem

global_config.disable_pypi_version_check = True

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
TENANT_ID = os.environ.get("TENANT_ID")
COGNITE_PROJECT = os.getenv("COGNITE_PROJECT")
CDF_CLUSTER = os.getenv("CDF_CLUSTER")

if not (CLIENT_ID and CLIENT_SECRET and TENANT_ID and COGNITE_PROJECT and CDF_CLUSTER):
    raise ValueError("CLIENT_ID/CLIENT_SECRET/TENANT_ID/COGNITE_PROJECT/CDF_CLUSTER can not be blank or None")

# Create CDF Client.
DATASET_EXTERNAL_ID = os.getenv("DATASET_EXTERNAL_ID", "dataset:integration_tests")
SCOPES = [f"https://{CDF_CLUSTER}.cognitedata.com/.default"]
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

_SOURCES = [
    "test_int_pandas_csv",
    "test_int_pandas_parquet",
    "test_int_pandas_json",
    "test_int_xarray_zarr",
    "test_int_dask_csv",
    "test_int_dask_parquet",
    "test_int_geopandas_parquet",
    "test_int_file_io",
]


@pytest.fixture(scope="session")
def client_config():
    auth_creds = OAuthClientCredentials(
        token_url=TOKEN_URL, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, scopes=SCOPES
    )

    client_cnf = ClientConfig(
        client_name="cdffs-integration-test-client",
        base_url=f"https://{CDF_CLUSTER}.cognitedata.com",
        project=COGNITE_PROJECT,
        credentials=auth_creds,
    )
    return client_cnf


@pytest.fixture(scope="session")
def cognite_client(client_config):
    return CogniteClient(client_config)


@pytest.fixture(scope="session")
def data_set_id(cognite_client):
    data_set = cognite_client.data_sets.retrieve(external_id=DATASET_EXTERNAL_ID)
    if not data_set:
        raise ValueError("Invalid dataset external Id")
    return data_set.id


def verify_file_status(cognite_client, source, external_ids):
    while True:
        if (
            upload_status := [
                x.uploaded for x in cognite_client.files.list(external_id_prefix=external_ids, source=source)
            ]
        ) and all(upload_status):
            break
        time.sleep(1)


def delete_files(cognite_client):
    for source in _SOURCES:
        list_of_ids = [x.external_id for x in cognite_client.files.list(source=source, limit=-1)]
        if list_of_ids:
            cognite_client.files.delete(external_id=list_of_ids)


@pytest.fixture(scope="session", autouse=True)
def cleanup(cognite_client):
    """Function to perform a clean up after every test."""
    delete_files(cognite_client)
    yield
    delete_files(cognite_client)


# Package: Pandas
# File-format: csv
def test_pandas_csv(cognite_client, client_config, data_set_id):
    """Test read/write operations using pandas."""
    inp_df = pd.DataFrame(np.arange(100).reshape(50, 2), columns=["x", "y"])
    file_metadata = FileMetadata(source="test_int_pandas_csv", mime_type="application/csv", data_set_id=data_set_id)
    inp_df.to_csv(
        "cdffs://tests/integration/pandas/csv/test_int_pandas.csv",
        index=False,
        storage_options={"connection_config": client_config, "file_metadata": file_metadata},
    )

    verify_file_status(cognite_client, "test_int_pandas_csv", "test_int_pandas.csv")

    res_df = pd.read_csv(
        "cdffs://tests/integration/pandas/csv/test_int_pandas.csv",
        storage_options={"connection_config": client_config},
    )

    assert inp_df.shape == res_df.shape
    assert inp_df.compare(res_df).empty


# Package: Pandas
# File-format: parquet
def test_pandas_parquet(cognite_client, client_config, data_set_id):
    """Test read/write operations using pandas."""
    inp_df = pd.DataFrame(np.arange(100).reshape(50, 2), columns=["x", "y"])
    file_metadata = FileMetadata(
        source="test_int_pandas_parquet", mime_type="application/octet-stream", data_set_id=data_set_id
    )
    inp_df.to_parquet(
        "cdffs://tests/integration/pandas/parquet/test_int_pandas.parquet",
        index=False,
        storage_options={"connection_config": client_config, "file_metadata": file_metadata},
    )

    verify_file_status(cognite_client, "test_int_pandas_parquet", "test_int_pandas.parquet")

    res_df = pd.read_parquet(
        "cdffs://tests/integration/pandas/parquet/test_int_pandas.parquet",
        storage_options={"connection_config": client_config},
    )

    assert inp_df.shape == res_df.shape
    assert inp_df.compare(res_df).empty


# Package: Pandas
# File-format: json
def test_pandas_json(cognite_client, client_config, data_set_id):
    """Test read/write operations using pandas."""
    inp_df = pd.DataFrame(np.arange(100).reshape(50, 2), columns=["x", "y"])
    file_metadata = FileMetadata(
        source="test_int_pandas_json", mime_type="application/octet-stream", data_set_id=data_set_id
    )
    inp_df.to_json(
        "cdffs://tests/integration/pandas/json/test_int_pandas.json",
        orient="records",
        storage_options={"connection_config": client_config, "file_metadata": file_metadata},
    )

    verify_file_status(cognite_client, "test_int_pandas_json", "test_int_pandas.json")

    res_df = pd.read_json(
        "cdffs://tests/integration/pandas/json/test_int_pandas.json",
        storage_options={"connection_config": client_config},
    )

    assert inp_df.shape == res_df.shape
    assert inp_df.compare(res_df).empty


# Package: xarray
# File-format: zarr
def test_xarray_zarr(cognite_client, client_config, data_set_id):
    """Test read/write operations using xarray."""
    inp_ds = xr.DataArray(
        np.random.randn(50, 2),
        dims=("x", "y"),
        coords={"x": np.random.uniform(low=7, high=40, size=(50,)), "y": np.random.uniform(low=7, high=40, size=(2,))},
    ).to_dataset(name="test_int_xarray_zarr")

    file_metadata = FileMetadata(
        source="test_int_xarray_zarr", mime_type="application/octet-stream", data_set_id=data_set_id
    )
    inp_ds.to_zarr(
        "cdffs://tests/integration/xarray/zarr/test_int_xarray.zarr",
        storage_options={"connection_config": client_config, "file_metadata": file_metadata},
    )

    verify_file_status(cognite_client, "test_int_xarray_zarr", "test_int_xarray.zarr")

    res_da = xr.open_zarr(
        "cdffs://tests/integration/xarray/zarr/test_int_xarray.zarr",
        storage_options={"connection_config": client_config},
    )
    assert inp_ds.dims == res_da.dims
    assert inp_ds.sizes == res_da.sizes
    assert inp_ds.identical(res_da)


# Package: dask
# File-format: csv (single file)
def test_dask_csv_single_file(cognite_client, client_config, data_set_id):
    """Test read/write operations using dask."""
    inp_df = dd.from_pandas(
        pd.DataFrame(np.arange(100).reshape(50, 2), columns=["x", "y"]), npartitions=1
    ).reset_index()
    file_metadata = FileMetadata(source="test_int_dask_csv", mime_type="application/csv", data_set_id=data_set_id)
    inp_df.to_csv(
        "cdffs://tests/integration/dask/csv/test_int_dask_single_file.csv",
        index=False,
        single_file=True,
        storage_options={"connection_config": client_config, "file_metadata": file_metadata},
    )

    verify_file_status(cognite_client, "test_int_dask_csv", "test_int_dask_single_file.csv")

    res_df = dd.read_csv(
        "cdffs://tests/integration/dask/csv/test_int_dask_single_file.csv",
        storage_options={"connection_config": client_config},
    )

    assert inp_df.eq(res_df).all(axis=None, skipna=False).all(axis=None).compute()


# Package: dask
# File-format: csv (multiple files)
def test_dask_csv_multiple_files(cognite_client, client_config, data_set_id):
    """Test read/write operations using dask."""
    inp_df = dd.from_pandas(
        pd.DataFrame(np.arange(200).reshape(100, 2), columns=["x", "y"]), npartitions=3
    ).reset_index()
    file_metadata = FileMetadata(source="test_int_dask_csv", mime_type="application/csv", data_set_id=data_set_id)
    inp_df.to_csv(
        "cdffs://tests/integration/dask/csv/test_int_dask_multiple_files.csv",
        index=False,
        storage_options={"connection_config": client_config, "file_metadata": file_metadata},
    )

    verify_file_status(cognite_client, "test_int_dask_csv", "test_int_dask_multiple_files.csv")

    res_df = dd.read_csv(
        "cdffs://tests/integration/dask/csv/test_int_dask_multiple_files.csv/*.part",
        storage_options={"connection_config": client_config},
    )

    assert inp_df.eq(res_df).all(axis=None, skipna=False).all(axis=None).compute()


# Package: dask
# File-format: parquet (single file)
def test_dask_parquet_single_file(cognite_client, client_config, data_set_id):
    """Test read/write operations using dask."""
    inp_df = dd.from_pandas(
        pd.DataFrame(np.arange(100).reshape(50, 2), columns=["x", "y"]), npartitions=1
    ).reset_index()
    file_metadata = FileMetadata(
        source="test_int_dask_parquet", mime_type="application/octet-stream", data_set_id=data_set_id
    )
    inp_df.to_parquet(
        "cdffs://tests/integration/dask/parquet/test_int_dask_single_file.parquet",
        storage_options={"connection_config": client_config, "file_metadata": file_metadata},
    )

    verify_file_status(cognite_client, "test_int_dask_parquet", "test_int_dask_single_file.parquet")

    res_df = dd.read_parquet(
        "cdffs://tests/integration/dask/parquet/test_int_dask_single_file.parquet/part.*.parquet",
        storage_options={"connection_config": client_config},
    )

    assert inp_df.eq(res_df).all(axis=None, skipna=False).all(axis=None).compute()


# Package: dask
# File-format: parquet (multiple files)
def test_dask_parquet_multiple_file(cognite_client, client_config, data_set_id):
    """Test read/write operations using dask."""
    inp_df = dd.from_pandas(
        pd.DataFrame(np.arange(100).reshape(50, 2), columns=["x", "y"]), npartitions=1
    ).reset_index()
    file_metadata = FileMetadata(
        source="test_int_dask_parquet", mime_type="application/octet-stream", data_set_id=data_set_id
    )
    inp_df.to_parquet(
        "cdffs://tests/integration/dask/parquet/test_int_dask_multiple_files.parquet",
        storage_options={"connection_config": client_config, "file_metadata": file_metadata},
    )

    verify_file_status(cognite_client, "test_int_dask_parquet", "test_int_dask_multiple_files.parquet")

    res_df = dd.read_parquet(
        "cdffs://tests/integration/dask/parquet/test_int_dask_multiple_files.parquet/part.*.parquet",
        storage_options={"connection_config": client_config},
    )

    assert inp_df.eq(res_df).all(axis=None, skipna=False).all(axis=None).compute()


# Package: geopandas
# File-format: parquet
def test_geopandas_parquet(cognite_client, client_config, data_set_id):
    """Test read/write operations using geopandas."""
    inp_df = gpd.GeoDataFrame.from_file(geodatasets.get_path("nybb"))
    file_metadata = FileMetadata(
        source="test_int_geopandas_parquet", mime_type="application/octet-stream", data_set_id=data_set_id
    )

    inp_df.to_parquet(
        "/tests/integration/geopandas/parquet/test_int_geopandas.parquet",
        filesystem=CdfFileSystem(connection_config=client_config, file_metadata=file_metadata),
    )

    verify_file_status(cognite_client, "test_int_geopandas_parquet", "test_int_geopandas.parquet")

    res_df = gpd.read_parquet(
        "cdffs://tests/integration/geopandas/parquet/test_int_geopandas.parquet",
        filesystem=CdfFileSystem(connection_config=client_config),
    )

    assert inp_df.shape == res_df.shape
    assert inp_df.compare(res_df).empty


# File IO
# File-format: csv
def test_file_io_write(cognite_client, client_config, data_set_id):
    data = """name,country,region,numberrange\nBeck Cash,Spain,New South Wales,7\n"""
    file_metadata = FileMetadata(
        source="test_int_file_io", mime_type="application/csv", data_set_id=data_set_id, metadata={"type": "raw"}
    )
    file_system = CdfFileSystem(connection_config=client_config, file_metadata=file_metadata)
    with file_system.open("/file_io/test_int_sample_file_01.csv", mode="wb") as write_file:
        write_file.write(data.encode("utf8"))

    verify_file_status(cognite_client, "test_int_file_io", "test_int_sample_file_01.csv")
    file_info = cognite_client.files.list(external_id_prefix="test_int_sample_file_01.csv", source="test_int_file_io")
    assert file_info[0].source == "test_int_file_io"
    assert file_info[0].directory == "/file_io"
    assert file_info[0].mime_type == "application/csv"
    assert file_info[0].metadata["type"] == "raw"

    del file_system


# File IO
# File-format: json
def test_file_io_write_with_metadata(cognite_client, client_config, data_set_id):
    file_metadata = FileMetadata(
        source="test_int_file_io", mime_type="application/json", data_set_id=data_set_id, metadata={"type": "processed"}
    )

    data = {"name": "Beck Cash", "country": "Spain", "region": "New South Wales", "numberrange": 7}
    file_system = CdfFileSystem(connection_config=client_config, file_metadata=file_metadata)
    with file_system.open(
        "/file_io/test_int_sample_file_02.json", mode="wb", file_metadata=file_metadata
    ) as write_file:
        write_file.write(json.dumps(data).encode("utf8"))

    verify_file_status(cognite_client, "test_int_file_io", "test_int_sample_file_02.json")

    file_info = cognite_client.files.list(external_id_prefix="test_int_sample_file_02.json", source="test_int_file_io")
    assert file_info[0].source == "test_int_file_io"
    assert file_info[0].directory == "/file_io"
    assert file_info[0].mime_type == "application/json"
    assert file_info[0].metadata["type"] == "processed"

    del file_system


# File IO
# File-format: csv,json
def test_file_io_list(cognite_client, client_config, data_set_id):
    file_system = CdfFileSystem(connection_config=client_config)
    file_list = file_system.ls("file_io/")
    assert sorted(file_list) == [
        "file_io",
        "file_io/test_int_sample_file_01.csv",
        "file_io/test_int_sample_file_02.json",
    ]

    file_list = file_system.ls("/file_io/")
    assert sorted(file_list) == [
        "file_io",
        "file_io/test_int_sample_file_01.csv",
        "file_io/test_int_sample_file_02.json",
    ]

    del file_system


# File IO
# File-format: csv,json
def test_file_io_list_file(cognite_client, client_config, data_set_id):
    file_system = CdfFileSystem(connection_config=client_config)
    file_list = file_system.ls("file_io/test_int_sample_file_01.csv")
    assert file_list == ["file_io/test_int_sample_file_01.csv"]

    file_list = file_system.ls("/file_io/test_int_sample_file_02.json")
    assert file_list == ["file_io/test_int_sample_file_02.json"]

    del file_system


# File IO
# File-format: csv,json
def test_file_io_rm_file(cognite_client, client_config, data_set_id):
    file_metadata = FileMetadata(
        source="test_int_file_io", mime_type="application/json", data_set_id=data_set_id, metadata={"type": "processed"}
    )
    file_system = CdfFileSystem(connection_config=client_config, file_metadata=file_metadata)
    data = {"name": "Beck Cash", "country": "Spain", "region": "New South Wales", "numberrange": 7}
    with file_system.open(
        "/file_io/test_int_sample_file_03.json", mode="wb", file_metadata=file_metadata
    ) as write_file:
        write_file.write(json.dumps(data).encode("utf8"))

    verify_file_status(cognite_client, "test_int_file_io", "test_int_sample_file_03.json")

    # Remove file
    file_system.rm("file_io/test_int_sample_file_03.json")

    # List file
    with pytest.raises(FileNotFoundError):
        file_system.rm("file_io/test_int_sample_file_03.json")

    del file_system
