import re
import zipfile
from pathlib import Path

import fsspec
from cognite.client.data_classes import FileMetadata

from cognite import cdffs  # noqa


def clean_filename(path):
    # Replace multiple slashes or colons with a single underscore
    cleaned = re.sub(r'[:/]+', '_', path)
    return cleaned


def main():
    source_fs = fsspec.filesystem("file")
    target_fs = fsspec.filesystem("cdffs")

    BUFFER_SIZE = 1 * 1024 * 1024  # 1MB

    source_files = [
        f
        for f in source_fs.glob(f"{Path(__file__).parent.parent.absolute()}/**/*")
        if Path(f).is_file()
    ]

    with target_fs.open(
            f"test.zip",
            "wb",
            file_metadata=FileMetadata(
                source="test",
                directory="/archive/",
                data_set_id=8576667485598960,
                mime_type="application/zip",
                metadata={"test": "test"}
            ),
            block_size=BUFFER_SIZE,
    ) as target_file:
        with zipfile.ZipFile(target_file, 'w') as zipf:
            for source_path in source_files:
                with source_fs.open(source_path, "rb") as source_file:
                    zip_file_name = clean_filename(source_path)
                    zipf.writestr(zip_file_name, source_file.read())


if __name__ == '__main__':
    main()
