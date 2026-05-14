"""Exceptions raised by the TypeDB checkpoint saver."""

from __future__ import annotations


class TypeDBSaverError(Exception):
    """Base class for errors raised by the TypeDB checkpoint saver."""


class ConflictError(TypeDBSaverError):
    """A commit conflict could not be resolved after retries."""
