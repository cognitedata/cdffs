<a href="https://cognite.com/">
  <img alt="Cognite" src="https://raw.githubusercontent.com/cognitedata/cognite-python-docs/master/img/cognite_logo_black.png" alt="Cognite logo" title="Cognite" align="right" height="80">
</a>

[![GitHub](https://img.shields.io/github/license/cognitedata/cdffs)](https://github.com/cognitedata/cdffs/blob/main/LICENSE)
[![Documentation Status](https://readthedocs.org/projects/cdffs/badge/?version=latest)](https://cdffs.readthedocs.io/en/latest/?badge=latest)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![codecov](https://codecov.io/gh/cognitedata/cdffs/branch/main/graph/badge.svg)](https://codecov.io/gh/cognitedata/cdffs)
![PyPI](https://img.shields.io/pypi/v/cognite-cdffs)

# cdffs

A file system interface (`cdffs`) to allow users to work with CDF Files using the [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) supported/compatible python packages (`pandas`, `xarray` etc).

`fsspec` provides an abstract file system interface to work with local/cloud storages and based on the protocol name (example, `s3` or `abfs`) provided in the path, `fsspec` translates the incoming requests to storage specific implementations and send the responses back to the upstream package to work with the desired data.

Refer [fsspec documentation](https://filesystem-spec.readthedocs.io/en/latest/#who-uses-fsspec) to get the list of all supported/compatible python packages.

## Installation

`cdffs` is available on PyPI. Install using,

```shell
pip install cognite-cdffs
```

## Usage

Important steps to follow when working with CDF Files using the `fsspec` supported python packages.

1) Import `cdffs` package
```python
  from cognite import cdffs  # noqa
```

2) Follow instructions from [Authentication](https://cdffs.readthedocs.io/en/latest/authentication.html) to authenticate.

3) Read/write the files from/to CDF using `fsspec` supported packages. Example,

    * Read `zarr` files using using `xarray`.

    ```python
    ds = xarray.open_zarr("cdffs://sample_data/test.zarr")
    ```
    * Write `zarr` files using `xarray`.

    ```python
    ds.to_zarr("cdffs://sample_data/test.zarr", storage_options={"file_metadata": metadata})
    ```

Refer [cdffs.readthedocs.io](https://cdffs.readthedocs.io) for more details.

## Vendoring

`cdffs` uses pydandic.v1 package using vendoring. It was mainly introduced to overcome version conflicts
related to the Cognite Notebooks.

## Contributing
Want to contribute? Check out [CONTRIBUTING](CONTRIBUTING.md).
