"""Custom exception hierarchy for the Incorporator package."""

class IncorporatorError(Exception):
    """Base exception for all Incorporator related errors."""
    pass

class IncorporatorFormatError(IncorporatorError):
    """Raised when data cannot be parsed into a dictionary (malformed JSON/CSV/XML)."""
    pass

class IncorporatorNetworkError(IncorporatorError):
    """Raised when the internal HTTP client exhausts retries."""
    pass

class IncorporatorSchemaError(IncorporatorError):
    """Raised when dynamic Pydantic model compilation fails during Dynamic Class Building."""
    pass