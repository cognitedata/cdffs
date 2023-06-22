
Guidelines
==========

* User must ensure an unique filename (even when the path is different) when working with CDF Files.

    .. list-table:: path examples.
       :header-rows: 1

       * - Examples
         - Valid/Invalid

       * - | `cdffs://sample_data/test.zarr`
           | `cdffs://test_data/test.zarr`
         - INVALID

       * - | `cdffs://sample_data/test.zarr`
           | `cdffs://test_data/test_data.zarr`
         - VALID

       * - | `cdffs://test/ocean/sample_data/atlantic.csv`
           | `cdffs://ocean/production_data/atlantic.csv`
         - INVALID

       * - | `cdffs://test/ocean/sample_data/atlantic_sample.csv`
           | `cdffs://ocean/production_data/atlantic_prod.csv`
         - VALID

* User must use a valid file extension when working with CDF Files.

    * `cdffs://sample_data/test.zarr`

    * `cdffs://sample_data/test.csv`

    * `cdffs://sample_data/test.parquet`

    | Note: It is still recommended to use a valid file extension even when the data is split into multiple files. for example, dask might partition the data into multiple part files. 0.part, 1.part etc and it is still recommended to use an extension as well. Optionally, if you wish to avoid using file extensions especially when the data is split into multiple files, user can explicitly specify directory field in metadata to pass information about the directory prefix. Example,

    .. code-block:: python

        file_metadata = FileMetadata(source="test_parquet", directory="/sample_data/", data_set_id=5149640835927738),
        dd.to_parquet("cdffs://sample_data/test", storage_options={"connection_config": client_cnf, "file_metadata": file_metadata})

* It is highly recommended to use unique directory prefixes for each file.

* It is highly recommended to add metadata information by using file_metadata in storage_options when writing the files to CDF.

* Users can choose their preferred cache_type to cache file contents when working with CDF Files. It will be defaulted to readahead cache. However, It is recommended to use cache_type as all in storage_options if a user intended to perform read heavy operations. Example,

    .. code-block:: python

        ds = xarray.open_zarr("cdffs://sample_data/test.zarr", storage_options={"connection_config": client_cnf, "cache_type": "all"})

* Users can choose to use a file specific metadata when opening a file for write using file-like API.

    .. code-block:: python

        with fs.open("test_data/test/workfile.csv", mode="wb",file_metadata=FileMetadata(source="test")) as f:
        f.write("test_data".encode("utf8"))

* Users can also choose to use `limit` when working with `ls`. It may be useful when using file-like API.

    .. code-block:: python

        fs.ls("test_data/test/", detail=True, limit=100)
