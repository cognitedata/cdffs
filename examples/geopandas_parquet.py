"""Example script for geopandas package."""
import os

import geopandas as gpd
from cognite.client import ClientConfig, global_config
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.files import FileMetadata

from cognite.cdffs import CdfFileSystem

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

    # Create a geopandas dataframe
    inp_df = gpd.GeoDataFrame.from_file(gpd.datasets.get_path("nybb"))
    file_metadata = FileMetadata(source="test", mime_type="application/octet-stream", data_set_id=DATASET_ID)

    # geopandas package use pyarrow to read/write the parquet files from the underlying storage.
    # As highlighted, pyarrow has it is own generic file system specification but it is compatible with fsspec.
    # We can still make use of cdffs, we need to instantiate a new CdfFileSystem with client config
    # and pass it as filesystem when reading/writing the files from/to CDF.

    # Write the parquet file using Geopandas to CDF Files.
    inp_df.to_parquet(
        "/sample/geopandas/sample.parquet",
        filesystem=CdfFileSystem(connection_config=client_cnf, file_metadata=file_metadata),
    )

    # Read the parquet file using Geopandas from CDF Files.
    res_df = gpd.read_parquet(
        "cdffs://sample/geopandas/sample.parquet", filesystem=CdfFileSystem(connection_config=client_cnf)
    )

    print(inp_df.shape, res_df.shape)


if __name__ == "__main__":
    main()
