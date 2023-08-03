#!/usr/bin/env python3
"""
Created on Mon Aug 15 00:17:07 2022

@author: ghiggi
"""
from importlib.metadata import PackageNotFoundError, version

__all__ = []

# Get version
try:
    __version__ = version("xencoding")
except PackageNotFoundError:
    # package is not installed
    pass
