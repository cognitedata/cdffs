Examples
========

Few examples with various fsspec supported/compatible python packages are given below.

.. contents::
   :local:

pandas
^^^^^^
Example to read/write `csv` file from/to CDF Files using `pandas` package. Environment variables are used to authenticate.

.. literalinclude:: ./../../examples/pandas_csv.py
   :language: python

xarray
^^^^^^
Example to read/write `zarr` files from/to CDF Files using `xarray` package.

.. literalinclude:: ./../../examples/xarray_zarr.py
   :language: python

dask
^^^^^^
Example to read/write `csv` file from/to CDF Files using `dask` package.

.. literalinclude:: ./../../examples/dask_csv.py
   :language: python

geopandas
^^^^^^^^^
Example to read/write `parquet` file from/to CDF Files using `geopandas` package.

Note: `geopandas` package use `pyarrow` to read/write the parquet files from the underlying storage. `pyarrow` has it is
own generic file system specification but it is compatible with fsspec. So, We can still make use of cdffs
but we need to instantiate a new CdfFileSystem with client config and pass it as filesystem when reading/writing
the files from/to CDF Files.

.. literalinclude:: ./../../examples/geopandas_parquet.py
   :language: python

zip file
^^^^^^^^^
Example to read folder from local filesystem and write `zip` file to CDF Files using `ZipFile` package.

.. literalinclude:: ./../../examples/zip.py
   :language: python
