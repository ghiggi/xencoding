#!/usr/bin/env python3
"""
Created on Thu Aug  3 09:07:51 2023

@author: ghiggi
"""
import numpy as np


def is_numcodecs(compressor):
    """Check is a numcodec compressor."""
    if type(compressor).__module__.find("numcodecs") == -1:
        return False
    return True


def _check_variables_names_type(variable_names):
    """Check variable_names type."""
    if not isinstance(variable_names, (list, str)):
        raise TypeError("'variable_names' must be a string or a list")
    if isinstance(variable_names, str):
        variable_names = [variable_names]
    if not all([isinstance(s, str) for s in variable_names]):
        raise TypeError("Specify all variable names as string within the 'variable_names' list.")
    return variable_names


def _check_compressor_type(compressor, variable_names):
    """Check compressor type."""
    if not (isinstance(compressor, (str, dict, type(None))) or is_numcodecs(compressor)):
        raise TypeError(
            "'compressor' must be a dictionary, numcodecs compressor, 'auto' string or None."
        )
    if isinstance(compressor, str):
        if not compressor == "auto":
            raise ValueError("If 'compressor' is specified as string, must be 'auto'.")
    if isinstance(compressor, dict):
        if not np.all(np.isin(list(compressor.keys()), variable_names)):
            raise ValueError(f"The 'compressor' dictionary must contain the keys {variable_names}")
    return compressor


def _check_default_compressor_type(default_compressor, variable_names):
    """Check default_compressor type."""
    if not (isinstance(default_compressor, (dict, type(None))) or is_numcodecs(default_compressor)):
        raise TypeError("'default_compressor' must be a numcodecs compressor or None.")
    if isinstance(default_compressor, dict):
        if not np.all(np.isin(list(default_compressor.keys()), variable_names)):
            raise ValueError(
                "The 'default_compressor' dictionary must contain the keys {variable_names}"
            )
    return default_compressor


def _check_compressor_dict(compressor):
    """Check compressor dictionary validity."""
    all_valid = [is_numcodecs(cmp) or isinstance(cmp, type(None)) for cmp in compressor.values()]
    if not all(all_valid):
        raise ValueError(
            "All compressors specified in the 'compressor' dictionary must be numcodecs (or None)."
        )
    return compressor


def check_compressor(compressor, variable_names, default_compressor=None):
    """Check compressor validity for zarr writing.

    compressor = None --> No compression.
    compressor = "auto" --> Use default_compressor if specified.
      Otherwise will default to ds.to_zarr() default compressor.
    compressor = <numcodecs class> --> Specify the same compressor to all Dataset variables
    compressor = {..} --> A dictionary specifying a compressor for each Dataset variable.

    default_compressor: None or numcodecs compressor. None will default to ds.to_zarr() default compressor.
    variable_names: list of xarray Dataset variables
    """
    variable_names = _check_variables_names_type(variable_names)
    compressor = _check_compressor_type(compressor, variable_names)
    default_compressor = _check_compressor_type(default_compressor, variable_names)

    # If a string --> "Auto" --> Apply default_compressor (if specified)
    if isinstance(compressor, str):
        if compressor == "auto":
            compressor = default_compressor

    # If a unique compressor, create a dictionary with the same compressor for all variables
    elif is_numcodecs(compressor) or isinstance(compressor, type(None)):
        compressor = {var: compressor for var in variable_names}

    # If a dictionary, check keys validity and compressor validity
    else:  # isinstance(compressor, dict):
        compressor = _check_compressor_dict(compressor)

    return compressor
