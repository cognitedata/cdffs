Welcome to cognite-cdffs's documentation!
=========================================

A file-system interface (`cdffs`) that allow users to work with CDF (Cognite Data Fusion) Files using `fsspec <https://filesystem-spec.readthedocs.io/en/latest/>`_
supported/compatible python packages (`pandas`, `xarray` etc). `cdffs` uses `cognite-sdk-python <https://cognite-sdk-python.readthedocs-hosted.com/en/latest/index.html>`_ to work with CDF Files.

Refer `fsspec documentation <https://filesystem-spec.readthedocs.io/en/latest/#who-uses-fsspec>`_ to get
the list of all supported/compatible python packages.

Installation
^^^^^^^^^^^^
To install this package(Recommended):

  .. code-block:: bash

   pip install cognite-cdffs[settings]

If you need `cdffs` to be compatible with pydantic-v1, choose to install the expected pydantic-v1 (`^1.10.7`) and use,

  .. code-block:: bash

   pip install cognite-cdffs


Quickstart
^^^^^^^^^^

Important steps to follow when working with CDF Files using the ``fsspec`` supported python packages.

1. Import ``cdffs`` package

  .. code-block:: python

    from cognite import cdffs

2. Follow instructions from :ref:`authentication` to authenticate.

3. Read/write the files from/to CDF using ``fsspec`` supported packages.

  Read `zarr` files using using `xarray`. (`Environment variables are used to authenticate`)

  .. code-block:: python

    ds = xarray.open_zarr("cdffs://sample_data/test.zarr")

  Write `zarr` files using `xarray`.

  .. code-block:: python

    metadata = FileMetadata(source="sample", data_set_id=1234567890)
    ds.to_zarr("cdffs://sample_data/test.zarr", storage_options={"file_metadata": metadata})


Contents
^^^^^^^^
.. toctree::
   cdffs.rst
   authentication.rst
   guidelines.rst
   examples.rst
   api.rst

