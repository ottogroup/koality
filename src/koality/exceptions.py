"""Custom exceptions for koality."""


class KoalityError(Exception):
    """Base exception class for Koality-related errors."""


class DatabaseError(KoalityError):
    """Exception raised for errors in the database operations."""
