#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 09:05:13 2023

@author: ghiggi
"""
import shutil 
from xencoding.zarr.checks.chunks import check_chunks
from xencoding.zarr.checks.zarr_store import _check_zarr_store


def rechunk_dataset(ds, chunks, target_store, temp_store, max_mem="2GB", force=False):
    """
    Rechunk on disk a xarray Dataset read lazily from a zarr store.

    Parameters
    ----------
    ds : xarray.Dataset
        A Dataset opened with open_zarr().
    chunks : dict
        Custom chunks of the new Dataset.
        If not specified for each Dataset variable, implicitly assumed.
    target_store : str
        Filepath of the zarr store where to save the new Dataset.
    temp_store : str
        Filepath of a zarr store where to save temporary data.
        This store is removed at the end of the rechunking operation.
    max_mem : str, optional
        The amount of memory (in bytes) that workers are allowed to use.
        The default is '1GB'.

    Returns
    -------
    None.

    """
    # TODO
    # - Add compressors options
    # compressor = Blosc(cname='zstd', clevel=1, shuffle=Blosc.BITSHUFFLE)
    # options = dict(compressor=compressor)
    # rechunk(..., target_options=options)
    ##------------------------------------------------------------------------.
    from rechunker import rechunk
    from dask.diagnostics import ProgressBar
    
    # Check zarr stores
    _check_zarr_store(target_store, force=force)
    _check_zarr_store(temp_store, force=True)  # remove if exists

    # Check chunks
    target_chunks = check_chunks(ds=ds, chunks=chunks, default_chunks=None)

    # Plan rechunking
    r = rechunk(
        ds,
        target_chunks=target_chunks,
        max_mem=max_mem,
        target_store=target_store,
        temp_store=temp_store,
    )

    # Execute rechunking
    with ProgressBar():
        r.execute()

    # Remove temporary store
    _check_zarr_store(temp_store, force=True)  # remove if exists
    

 