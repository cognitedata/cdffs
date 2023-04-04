"""Example script for pandas package."""
import os

import numpy as np
import pandas as pd
from cognite.client import global_config
from cognite.client.data_classes.files import FileMetadata

from cognite import cdffs  # noqa

global_config.disable_pypi_version_check = True

DATASET_ID = os.environ.get("DATASET_ID")


def main():
    # Create a dataframe
    df = pd.DataFrame({"x": np.arange(1000), "y": np.arange(1000)})
    file_metadata = FileMetadata(source="pandas_test", mime_type="text/csv", data_set_id=DATASET_ID)

    # Write the data using pandas to CDF Files.
    df.to_csv(
        "cdffs://pandas_test/out/pandas_df.csv",
        index=False,
        storage_options={"file_metadata": file_metadata},
    )

    # Read the data using pandas from CDF Files.
    df2 = pd.read_csv("cdffs://pandas_test/out/pandas_df.csv")

    print(df.shape, df2.shape)


if __name__ == "__main__":
    main()
