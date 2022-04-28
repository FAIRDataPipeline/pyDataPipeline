"""
    custom exception for netCDF wrappers
"""


class Error(Exception):
    """Base class for other exceptions"""

    pass


class AttributeSizeError(Error):
    """Raised when the len attribute related input are different"""

    pass


class DataSizeError(Error):
    """Raised when the length of data related input are different"""

    pass
