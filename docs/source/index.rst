Welcome to cognite-cdffs's documentation!
=========================================

A file-system interface (`cdffs`) to allow users to work with CDF Files using 
the `fsspec <https://filesystem-spec.readthedocs.io/en/latest/>`_ 
supported/compatible python packages (`pandas`, `xarray` etc). It builds on top of `cognite-sdk-python <https://cognite-sdk-python.readthedocs-hosted.com/en/latest/index.html>`_

Refer `fsspec documentation <https://filesystem-spec.readthedocs.io/en/latest/#who-uses-fsspec>`_ to get 
the list of all supported/compatible python packages.

Installation
^^^^^^^^^^^^
To install this package:

.. code-block:: bash

   pip install cognite-cdffs


Quickstart
^^^^^^^^^^

Three important steps to follow when working with CDF Files using the fsspec supported python packages.

1. Import cdffs package

.. code-block:: python

    from cognite import cdffs

2. Create a client config to connect with CDF. Refer `ClientConfig <https://cognite-sdk-python.readthedocs-hosted.com
/en/latest/cognite.html#cognite.client.config.ClientConfig>`_ from Cognite Python SDK documentation on how 
to create a client config.

Example using oauth credentials.

.. code-block:: python 

    # Get TOKEN_URL, CLIENT_ID, CLIENT_SECRET, COGNITE_PROJECT, 
    # CDF_CLUSTER, SCOPES from environment variables.

    oauth_creds = OAuthClientCredentials(
        token_url=TOKEN_URL, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, scopes=SCOPES
    )
    client_cnf = ClientConfig(
        client_name="cdf-client",
        base_url=f"https://{CDF_CLUSTER}.cognitedata.com",
        project=COGNITE_PROJECT,
        credentials=oauth_creds,
        timeout=60,
    )

3. Pass the client config as :code:`connection_config` in :code:`storage_options` when reading/writing the files.

   Read `zarr` files using using `xarray`.

   .. code-block:: python

      ds = xarray.open_zarr("cdffs://sample_data/test.zarr", storage_options={"connection_config": client_cnf})

   Write `zarr` files using `xarray`.

   .. code-block:: python

      ds.to_zarr("cdffs://sample_data/test.zarr", storage_options={"connection_config": client_cnf, "file_metadata": metadata})

Contents
^^^^^^^^
.. toctree::
   cdffs.rst
   guidelines.rst
   examples.rst
   api.rst

