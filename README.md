# cdffs
File-system interface for CDF Files.

Intention of this project is to allow users to work with CDF Files using the popular python packages.

## How to use:

When working with `xarray` or `pandas`

* Import the cdffs

```python
from cognite import cdffs
```

* Create the client config to connect with CDF. Refer [ClientConfig](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/cognite.html#cognite.client.config.ClientConfig) on how to create a client config.

```python
client_cnf = ClientConfig(
    client_name="cdf-client",
    base_url=f"https://<cluster>.cognitedata.com",
    project=<cdf project>,
    credentials=<cdf credentials>,
    timeout=60,
)
```

* Pass the client config as `connection_config` in `storage_options` when read/writing the data.

```python
ds = xarray.open_zarr("cdffs://sample_data/test.zarr", storage_options={"connection_config": client_cnf})
```

or

```python
df = pd.read_csv("cdffs://pandas_test/out/pandas_df.csv", storage_options={"connection_config": client_cnf})
```

## Sample Scripts

### Sample script to work CDF files using `xarray`.

```python
import os
import numpy as np
import pandas as pd
import xarray
from cognite import cdffs
from cognite.client.credentials import OAuthClientCredentials
from cognite.client import ClientConfig

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
COGNITE_PROJECT = os.environ.get("COGNITE_PROJECT")
TENANT_ID = os.environ.get("TENANT_ID")
CDF_CLUSTER = os.environ.get("CDF_CLUSTER")

# Create CDF Client.
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

# Write the data using xarray
df = pd.DataFrame({"x": np.arange(1000), "y": np.arange(1000)})
ds1 = df.to_xarray()

# Write the data to CDF Files.
metadata = {"source": "numpy_array"}
ds1.to_zarr("cdffs://sample35/test.zarr", storage_options={"connection_config": client_cnf, "metadata": metadata})

# Read the data back from CDF Files.
ds2 = xarray.open_zarr("cdffs://sample35/test.zarr", storage_options={"connection_config": client_cnf})
```

### Sample script to work CDF files using `pandas`.

```python
import os
import numpy as np
import pandas as pd
from cognite import cdffs
from cognite.client.credentials import OAuthClientCredentials
from cognite.client import ClientConfig

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
COGNITE_PROJECT = os.environ.get("COGNITE_PROJECT")
TENANT_ID = os.environ.get("TENANT_ID")
CDF_CLUSTER = os.environ.get("CDF_CLUSTER")

# Create CDF Client.
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

# Write the data using xarray
df = pd.DataFrame({"x": np.arange(1000), "y": np.arange(1000)})
metadata = {"source": "pandas_test"}
df.to_csv(
    "cdffs://pandas_test/out/pandas_df.csv",
    index=False,
    storage_options={"connection_config": client_cnf, "metadata": metadata},
)

# read the data using pandas
df2 = pd.read_csv("cdffs://pandas_test/out/pandas_df.csv", storage_options={"connection_config": client_cnf})
```
