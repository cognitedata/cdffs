"""Initialize the cdffs package."""
import fsspec

from .spec import CdfFileSystem

__version__ = "0.1.0"
__all__ = ["CdfFileSystem"]

fsspec.register_implementation("cdffs", CdfFileSystem)
