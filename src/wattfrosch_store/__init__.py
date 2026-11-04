"""wattfrosch-store: measurement storage for the wattfrosch pipeline."""

from __future__ import annotations


# Defined before the submodule imports below to avoid a circular import
# (loader/db/io import it back from this package).
class StoreError(Exception):
    """Base class for all wattfrosch-store errors."""


class ContextStoreError(StoreError):
    """Store error carrying structured ``context`` alongside the message."""

    def __init__(self, message: str, context: dict[str, object]) -> None:
        super().__init__(message)
        self.context = context


from wattfrosch_store.loader import LoadResult, load_records

__all__ = ["ContextStoreError", "LoadResult", "StoreError", "load_records"]
