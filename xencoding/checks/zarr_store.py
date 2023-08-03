#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 09:13:22 2023

@author: ghiggi
"""
import os 
import shutil 


def _check_zarr_store(target_store, force):
    """Check the Zarr target_store do not exist already."""
    if os.path.exists(target_store):
        if force:
            shutil.rmtree(target_store)
        else:
            raise ValueError(
                f"A zarr store already exists at {target_store}. If you want to overwrite, specify force=True"
            )