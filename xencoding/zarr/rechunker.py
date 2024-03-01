#!/usr/bin/env python3
"""
Created on Thu Aug  3 09:05:13 2023

@author: ghiggi
"""
from xencoding.checks.chunks import check_chunks
from xencoding.checks.zarr_store import _check_zarr_store


def rechunk_dataset(ds, target_chunks, target_store, temp_store, max_mem, 
                    force=False, **to_zarr_kwargs):
    """
    Rechunk on disk a xarray Dataset read lazily from a zarr store.

    Parameters
    ----------
    ds : xarray.Dataset
        A Dataset opened with open_zarr().
    target_chunks : dict
        Custom chunks of the new Dataset.
        If not specified for each Dataset variable, implicitly assumed.
    target_store : str, zarr.Store
        Filepath of the zarr store (or zarr.Store object) where to save the new Dataset.
    temp_store : str
        Filepath of a zarr store where to save temporary data.
        This store is removed at the end of the rechunking operation.
    max_mem : str, optional
        The amount of memory (in bytes) that each workers is allowed to use.
        A string (e.g. 100MB) can also be used.
    force : bool 
        If the target_store already exists, if force=True it is removed, otherwise 
        an error is raised. 
    to_zarr_kwargs: dict
        Arguments to pass to the ds.to_zarr() functions.
        This includes the encoding dictionary ! 
    
    Returns
    -------
    None.

    """
    ##------------------------------------------------------------------------.
    from dask.diagnostics import ProgressBar
    from rechunker import rechunk

    # Check zarr stores
    _check_zarr_store(target_store, force=force)
    _check_zarr_store(temp_store, force=True)  # remove if exists

    # Check chunks
    # - If target chunk is a dictionary, should also include the dimensions chunks !  
    # - currently check_chunks does not include dimensions ! 
    # --> SOLVE THE BUG. Can inspire from parse_target_chunks_from_dim_chunks
    
    # --> rechunker.parse_target_chunks_from_dim_chunks
    # from rechunker.api import parse_target_chunks_from_dim_chunks
    # target_chunks = parse_target_chunks_from_dim_chunks(ds, target_chunks)
    
    # target_chunks = check_chunks(ds=ds, chunks=target_chunks, default_chunks=None)
    # print(target_chunks)
    
    # Plan rechunking
    try: 
        r = rechunk(
            ds,
            target_chunks=target_chunks,
            max_mem=max_mem,
            target_store=target_store,
            temp_store=temp_store,
            target_options=to_zarr_kwargs,
        )
    
        # Execute rechunking
        with ProgressBar():
            r.execute()
    except Exception:
        # Remove temporary store
        _check_zarr_store(temp_store, force=True)  # remove if exists
        raise ValueError("Rechunking failed!")
