class KoalityError(Exception):
    """Base exception class for Koality-related errors."""

    pass


class DatabaseError(KoalityError):
    """Exception raised for errors in the database operations."""

    pass
