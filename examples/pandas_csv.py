import os

import numpy as np
import pandas as pd
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

# Create a dataframe
df = pd.DataFrame({"x": np.arange(1000), "y": np.arange(1000)})
file_metadata = FileMetadata(source="pandas_test", mime_type="text/csv", data_set_id=DATASET_ID)

# Write the data using pandas to CDF Files.
df.to_csv(
    "cdffs://pandas_test/out/pandas_df.csv",
    index=False,
    storage_options={"connection_config": client_cnf, "file_metadata": file_metadata},
)

# Read the data using pandas from CDF Files.
df2 = pd.read_csv("cdffs://pandas_test/out/pandas_df.csv", storage_options={"connection_config": client_cnf})
