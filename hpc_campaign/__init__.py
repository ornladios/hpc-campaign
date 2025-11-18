"""
A high-level interface for accessing and managing project data.

This package provides utility classes and functions for handling lists,
campaign information, and creating compressed archive indexes.
It offers access to:
- List: A list of campaign archive.
- Info: A list of all datasets in a campaign archive.
- CreateTarIndex: Utility for generating tarball indexes.
"""
from .list import List
from .manager import CampaignInfo as Info
from .taridx import CreateTarIndex

__all__ = [
    "List",
    "Info",
    "CreateTarIndex",
]

__version__ = "0.6.0"
