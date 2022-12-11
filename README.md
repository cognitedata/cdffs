
<a href="https://cognite.com/">
    <img src="https://raw.githubusercontent.com/cognitedata/cognite-python-docs/master/img/cognite_logo_white.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

# cdffs

A file-system interface (`cdffs`) to allow users to work with CDF Files using the [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) supported/compatible python packages (`pandas`, `xarray` etc).

`fsspec` provides an abstract file system interface to work with local/cloud storages and based on the protocol name (example, `s3` or `abfs`) provided in the path, `fsspec` translates the incoming requests to storage specific implementations and send the responses back to the upstream package to work with the desired data.

Refer [Who uses fsspec](https://filesystem-spec.readthedocs.io/en/latest/#who-uses-fsspec) to get the list of all supported/compatible python packages.

## Installation

`cdffs` is available on PyPI. Install using, 

```shell
pip install cognite-cdffs
```

## Usage

Three important steps to follow when working with CDF Files using the fsspec supported python packages. 

1) Import `cdffs` package

```python
from cognite import cdffs
```

2) Create a client config to connect with CDF. 

```python
client_cnf = ClientConfig(
    client_name="cdf-client",
    base_url=f"https://<cluster>.cognitedata.com",
    project=<cdf project>,
    credentials=<cdf credentials>,
    timeout=60,
)
```

Refer [ClientConfig](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/cognite.html#cognite.client.config.ClientConfig) from Cognite Python SDK documentation on how to create a client config.

3) Pass the client config as `connection_config` in `storage_options` when reading/writing the data.

    * Read zarr files using using `xarray`.

```python
ds = xarray.open_zarr("cdffs://sample_data/test.zarr", storage_options={"connection_config": client_cnf})
```
    * Write zarr files using `xarray`.

```python
ds.to_zarr("cdffs://sample_data/test.zarr", storage_options={"connection_config": client_cnf, "file_metadata": metadata})
```
