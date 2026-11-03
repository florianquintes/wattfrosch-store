"""wattfrosch-store: measurement storage for the wattfrosch pipeline."""

from __future__ import annotations


class StoreError(Exception):
    """Base class for all wattfrosch-store errors."""


__all__ = ["StoreError"]
