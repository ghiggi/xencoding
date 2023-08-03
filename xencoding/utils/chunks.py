#!/usr/bin/env python3
"""
Created on Thu Aug  3 09:19:02 2023

@author: ghiggi
"""


def get_dataset_chunks(ds):
    """Get dataset chunks dictionary."""
    variable_names = list(ds.data_vars.keys())
    chunks = {}
    for var in variable_names:
        if ds[var].chunks is not None:
            chunks[var] = {dim: v[0] for dim, v in zip(ds[var].dims, ds[var].chunks, strict=True)}
        else:
            chunks[var] = None
    return chunks
