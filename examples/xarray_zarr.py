"""Example script for xarray package."""
import os

import numpy as np
import pandas as pd
import xarray
from cognite.client import ClientConfig, global_config
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.files import FileMetadata

from cognite import cdffs  # noqa

global_config.disable_pypi_version_check = True

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
COGNITE_PROJECT = os.environ.get("COGNITE_PROJECT")
TENANT_ID = os.environ.get("TENANT_ID")
CDF_CLUSTER = os.environ.get("CDF_CLUSTER")
DATASET_ID = os.environ.get("DATASET_ID")

# Create a CDF Client Config
SCOPES = [f"https://{CDF_CLUSTER}.cognitedata.com/.default"]
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"


def main():
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

    # Create a dataset
    df = pd.DataFrame({"x": np.arange(1000), "y": np.arange(1000)})
    ds1 = df.to_xarray()

    # Write the zarr files using xarray to CDF Files.
    file_metadata = FileMetadata(source="sample_zarr", mime_type="application/octet-stream", data_set_id=DATASET_ID)
    ds1.to_zarr(
        "cdffs://sample/test.zarr", storage_options={"connection_config": client_cnf, "file_metadata": file_metadata}
    )

    # Read the zarr files using xarray from CDF Files.
    ds2 = xarray.open_zarr("cdffs://sample/test.zarr", storage_options={"connection_config": client_cnf})

    print(ds1.info, ds2.info)


if __name__ == "__main__":
    main()
