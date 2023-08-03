#!/usr/bin/env python3
"""
Created on Thu Jul 29 16:11:33 2021.

@author: ghiggi
"""
import os

import zarr

from xencoding.checks.chunks import check_chunks
from xencoding.checks.compressor import check_compressor
from xencoding.checks.rounding import check_rounding


def write_zarr(
    zarr_fpath,
    ds,
    chunks="auto",
    default_chunks=None,
    compressor="auto",
    default_compressor=None,
    rounding=None,
    consolidated=True,
    append=False,
    append_dim=None,
    show_progress=True,
):
    """Write Xarray Dataset to zarr with custom chunks and compressor per Dataset variable."""
    # Good to know: chunks=None: keeps current chunks, chunks='auto' rely on xarray defaults
    # append=True: if zarr_fpath do not exists, set to False (for the first write)
    ##-------------------------------------------------------------------------.
    from dask.diagnostics import ProgressBar

    ### Check zarr_fpath and append options
    # - Check fpath
    if not zarr_fpath.endswith(".zarr"):
        zarr_fpath = zarr_fpath + ".zarr"
    # - Check append options
    ZARR_EXIST = os.path.exists(zarr_fpath)
    if not isinstance(append, bool):
        raise TypeError("'append' must be either True or False'.")
    # If append = False and a Zarr store already exist --> Raise Error
    if not append and ZARR_EXIST:
        raise ValueError(zarr_fpath + " already exists!")
    # If the Zarr store do not exist yet but append = True, append is turned to False
    # --> Useful when calling this function to write data subset by subset
    if append and not ZARR_EXIST:
        append = False
    if append:
        if not isinstance(append_dim, str):
            raise TypeError("Please specify the 'append_dim' (as a string).")
    else:
        append_dim = None

    ##------------------------------------------------------------------------.
    ### - Define file chunking
    chunks = check_chunks(ds, chunks=chunks, default_chunks=default_chunks)

    ##------------------------------------------------------------------------.
    # - Define compressor and filters
    compressor = check_compressor(
        compressor=compressor,
        default_compressor=default_compressor,
        variable_names=list(ds.data_vars.keys()),
    )

    ##------------------------------------------------------------------------.
    # - Define rounding option
    rounding = check_rounding(rounding=rounding, variable_names=list(ds.data_vars.keys()))

    ##------------------------------------------------------------------------.
    # - Rounding (if required)
    if rounding is not None:
        if isinstance(rounding, int):
            ds = ds.round(decimals=rounding)
        elif isinstance(rounding, dict):
            for var, decimal in rounding.items():
                if decimal is not None:
                    ds[var] = ds[var].round(decimal)
        else:
            raise NotImplementedError("'rounding' should be int, dict or None.")
    ##------------------------------------------------------------------------.
    # - Remove previous encoding filters
    # - https://github.com/pydata/xarray/issues/3476
    # for var in ds.data_vars.keys():
    #     ds[var].encoding['filters'] = None
    for dim in list(ds.dims.keys()):
        ds[dim].encoding["filters"] = None  # Without this, bug when coords are str objects

    ##------------------------------------------------------------------------.
    # - Add chunk encoding the dataset
    for var, chunk in chunks.items():
        if chunk is not None:
            ds[var] = ds[var].chunk(chunk)

    ##------------------------------------------------------------------------.
    # - Add compressor encoding to each DataArray
    for var, comp in compressor.items():
        ds[var].encoding["compressor"] = comp

    ##------------------------------------------------------------------------.
    ### - Write zarr files
    compute = not show_progress
    # - Write data to new zarr store
    if not append:
        # - Define zarr store
        zarr_store = zarr.DirectoryStore(zarr_fpath)
        r = ds.to_zarr(
            store=zarr_store,
            mode="w",  # overwrite if exists already
            synchronizer=None,
            group=None,
            consolidated=consolidated,
            compute=compute,
        )
        if show_progress:
            with ProgressBar():
                r.compute()
    # - Append data to existing zarr store
    # ---> !!! Attention: It do not check if data are repeated !!!
    else:
        r = ds.to_zarr(
            store=zarr_fpath,
            mode="a",
            append_dim=append_dim,
            synchronizer=None,
            group=None,
            consolidated=consolidated,
            compute=compute,
        )
        if show_progress:
            with ProgressBar():
                r.compute()
