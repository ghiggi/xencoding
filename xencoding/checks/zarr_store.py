#!/usr/bin/env python3
"""
Created on Thu Aug  3 09:13:22 2023

@author: ghiggi
"""
import os
import shutil


def _get_store_path(target_store):
    if isinstance(target_store, str): 
        return target_store
    try: 
        path = target_store.path
    except Exception: 
        raise ValueError("Expecting a Zarr.store or a string specifying the store path.")
    return path 


def _check_zarr_store(target_store, force):
    """Check the Zarr target_store do not exist already."""
    store_path = _get_store_path(target_store)
    if os.path.exists(store_path):
        if force:
            if os.path.isdir(store_path):
                shutil.rmtree(store_path)
            else: 
                os.remove(store_path)
        else:
            raise ValueError(
                f"A zarr store already exists at {target_store}. If you want to overwrite, specify force=True"
            )
