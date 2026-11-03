"""wattfrosch-store: measurement storage for the wattfrosch pipeline."""

from __future__ import annotations


class StoreError(Exception):
    """Base class for all wattfrosch-store errors."""


from wattfrosch_store.loader import LoadResult, load_records

__all__ = ["LoadResult", "StoreError", "load_records"]
