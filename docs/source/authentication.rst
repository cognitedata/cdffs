.. _authentication:

Authentication
==============

There are multiple ways users can authenticate to CDF.

1. Use environment variables as listed in :ref:`oauth_credentials` or :ref:`token` sections to authenticate.

2. Use environment file ``.env`` or other custom environment file with variable names listed in :ref:`oauth_credentials` or :ref:`token` sections to authenticate.

  When using **custom** environment file name, pass the environment file name as `env_file` in `storage_options` when
  working with ``fsspec`` supported packages. Example,

  .. code-block:: python

    import pandas as pd
    df = pd.read_csv("cdffs://pandas/test_data.csv", storage_options={"env_file": "/var/secrets.env"})

**Note:** When using bearer token as environment variable, token expiry is still limited to the expiry time set on the
token. Use `connection_config` to introduce auto refresh.

3. Authenticate by passing CDF `ClientConfig` as `connection_config` in `storage_options` when working with ``fsspec`` supported packages.

  Refer `ClientConfig <https://cognite-sdk-python.readthedocs-hosted.com
  /en/latest/cognite.html#cognite.client.config.ClientConfig>`_ from Cognite Python SDK documentation on how
  to create a client config.

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

    import pandas as pd
    df = pd.read_csv("cdffs://pandas/test_data.csv", storage_options={"connection_config": client_cnf})

.. _oauth_credentials:

OAuth Credentials
^^^^^^^^^^^^^^^^^

Set oauth credentials as environment variables. variable names must match the below names.

  .. list-table::
     :header-rows: 1

     * - Variable names
       - Description

     * - TOKEN_URL
       - OAuth token url

     * - CLIENT_ID
       - Application client id.

     * - CLIENT_SECRET
       - Application client secret

     * - COGNITE_PROJECT
       - CDF Project Name

     * - CDF_CLUSTER
       - CDF Cluster Name

     * - SCOPES
       - list of scopes.


.. _token:

Token
^^^^^

Set bearer token along with CDF project/cluster details as environment variables.
variable names must match the below names.

  .. list-table::
     :header-rows: 1

     * - Variable names
       - Description

     * - TOKEN
       - Bearer Token

     * - COGNITE_PROJECT
       - CDF Project Name

     * - CDF_CLUSTER
       - CDF Cluster Name
