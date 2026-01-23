"""avian_biosurveillance package

Minimal package initializer so the project can be installed with Poetry.
"""
__all__ = ["__version__"]

# Keep version in one place (matches pyproject.toml)
__version__ = "0.1.0"


def info() -> str:
    """Return a short informational string for quick manual checks.

    Example:
        >>> import avian_biosurveillance
        >>> avian_biosurveillance.info()
        'avian_biosurveillance 0.1.0'
    """
    return f"avian_biosurveillance {__version__}"
