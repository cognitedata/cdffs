"""Initialize the cdffs package."""
import fsspec

from .spec import CdfFileSystem

__version__ = "0.2.10"
__all__ = ["CdfFileSystem"]

fsspec.register_implementation(CdfFileSystem.protocol, CdfFileSystem)
