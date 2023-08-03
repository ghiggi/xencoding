#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 09:20:20 2023

@author: ghiggi
"""
import numpy as np


def check_rounding(rounding, variable_names):
    """Check rounding validity.

    rounding = None --> No rounding.
    rounding = int --> All variables will be round to the specified decimals.
    rounding = dict --> Specify specific rounding for each variable
    """
    ##------------------------------------------------------------------------.
    # Check variable_names type
    if not isinstance(variable_names, (list, str)):
        raise TypeError("'variable_names' must be a string or a list.")
    if isinstance(variable_names, str):
        variable_names = [variable_names]
    if not all([isinstance(s, str) for s in variable_names]):
        raise ValueError(
            "Specify all variable names as string within the 'variable_names' list."
        )
    # Check rounding type
    if not isinstance(rounding, (int, dict, type(None))):
        raise TypeError("'rounding' must be a dictionary, integer or None.")
    ##------------------------------------------------------------------------.
    # If a dictionary, check valid keys and valid compressor
    if isinstance(rounding, dict):
        if not np.all(np.isin(list(rounding.keys()), variable_names)):
            raise ValueError(
                "The 'rounding' dictionary must contain the keys {}.".format(
                    variable_names
                )
            )
        if not all([isinstance(v, (int, type(None))) for v in rounding.values()]):
            raise ValueError(
                "The rounding decimals specified in the 'rounding' dictionary must be integers (or None)."
            )
        if any([v < 0 for v in rounding.values() if v is not None]):
            raise ValueError(
                "The rounding decimals specified in the 'rounding' dictionary must be positive integers (or None)."
            )
    ##------------------------------------------------------------------------.
    # If a unique compressor, create a dictionary with the same compressor for all variables
    if isinstance(rounding, int):
        if rounding < 0:
            raise ValueError("'rounding' decimal value must be larger than 0.")
    ##------------------------------------------------------------------------.
    return rounding
