#!/usr/bin/env python3
"""
Created on Thu Aug  3 09:02:52 2023

@author: ghiggi
"""
import itertools
import shutil
import time

import numcodecs
import numpy as np
import xarray as xr
import zarr

from xencoding.zarr.writer import write_zarr


###############################################
#### Utils for zarr io profiling and optim ####
###############################################
#### Storage
def _get_zarr_array_info_dict(arr):
    return {k: v for k, v in arr.info_items()}


def _get_zarr_array_storage_ratio(arr):
    # arr.info.obj.nbytes/arr.info.obj.nbytes_stored
    return float(_get_zarr_array_info_dict(arr)["Storage ratio"])


def _get_zarr_nbytes_stored(arr):
    return round(arr.info.obj.nbytes_stored / 1024 / 1024, 3)


def _get_zarr_nbytes(arr):
    return round(arr.info.obj.nbytes / 1024 / 1024, 3)


def get_nbytes_stored_zarr_variables(fpath):
    """Return nbytes stored each variable."""
    ds = xr.open_zarr(fpath)
    source_group = zarr.open(fpath)
    variables = list(ds.data_vars.keys())
    nbytes_stored = {}
    for variable in variables:
        nbytes_stored[variable] = _get_zarr_nbytes_stored(source_group[variable])
    return nbytes_stored


def get_nbytes_stored_zarr_coordinates(fpath):
    """Return storage ratio for each variable."""
    ds = xr.open_zarr(fpath)
    source_group = zarr.open(fpath)
    coords = list(ds.coords.keys())
    nbytes_stored = {}
    for coord in coords:
        nbytes_stored[coord] = _get_zarr_nbytes_stored(source_group[coord])
    return nbytes_stored


def get_storage_ratio_zarr(fpath):
    """Return storage ratio for the entire store."""
    stored_nbytes_coords = get_nbytes_stored_zarr_coordinates(fpath)
    stored_nbytes_variables = get_nbytes_stored_zarr_variables(fpath)
    stored_nbytes = sum(stored_nbytes_coords.values()) + sum(stored_nbytes_variables.values())
    ds = xr.open_zarr(fpath)
    nbytes = ds.nbytes / 1024 / 1024
    storage_ratio = nbytes / stored_nbytes
    storage_ratio = round(storage_ratio, 3)
    return storage_ratio


##----------------------------------------------------------------------------.
#### Memory size
def get_memory_size_dataset(ds):
    """Return size in MB of variables and coordinates."""
    size_dict = {k: ds[k].nbytes / 1024 / 1024 for k in list(ds.keys()) + list(ds.coords.keys())}
    return size_dict


def get_memory_size_dataset_variables(ds):
    """Return the memory size in MB of the Dataset variables."""
    size_dict = {k: ds[k].nbytes / 1024 / 1024 for k in list(ds.keys())}
    return size_dict


def get_memory_size_dataset_coordinates(ds):
    """Return the memory size in MB of the Dataset coordinates."""
    size_dict = {k: ds[k].nbytes / 1024 / 1024 for k in list(ds.coords.keys())}
    return size_dict


def get_memory_size_zarr(fpath, isel_dict={}):
    """Return size in MB of variables and coordinates."""
    ds = xr.open_zarr(fpath)
    ds = ds.isel(isel_dict)
    return get_memory_size_dataset(ds)


def get_memory_size_zarr_variables(fpath, isel_dict={}):
    """Return the memory size in MB of the Zarr variables."""
    ds = xr.open_zarr(fpath)
    ds = ds.isel(isel_dict)
    return get_memory_size_dataset_variables(ds)


def get_memory_size_zarr_coordinates(fpath, isel_dict={}):
    """Return the memory size in MB of the Zarr coordinates."""
    ds = xr.open_zarr(fpath)
    ds = ds.isel(isel_dict)
    return get_memory_size_dataset_coordinates(ds)


def get_memory_size_chunk(x):
    """Return the size in MB of a single chunk.

    If x is an xr.Dataset, it returns a dictionary with the chunk size of each variable.
    """
    import dask

    if isinstance(x, xr.Dataset):
        size_dict = {}
        for var in list(x.data_vars.keys()):
            size_dict[var] = get_memory_size_chunk(x[var])
        return size_dict
    if isinstance(x, xr.DataArray):
        # If chunked: return the size of the chunk
        if x.chunks is not None:
            isel_dict = {dim: slice(0, chunks[0]) for dim, chunks in zip(x.dims, x.chunks)}
            x = x.isel(isel_dict)
            return x.nbytes / 1024 / 1024
        # If not chunked, return the size of the entire array
        else:
            return x.nbytes / 1024 / 1024
    elif isinstance(x, dask.array.core.Array):
        # slice_list = [slice(None, chunk[0]) for chunk in x.chunks]
        # x[*slice_list]
        raise NotImplementedError("Dask arrays")
    elif isinstance(x, np.ndarray):
        return x.nbytes / 1024 / 1024
    else:
        raise NotImplementedError("What array you provided?")


# -----------------------------------------------------------------------------.
#### IO Timing
import os
from xencoding.zarr.numcodecs import (
    get_valid_blosc_algorithms,
    get_compressor,
)
from xencoding.checks.zarr_compressor import check_compressor
from xencoding.zarr.writer import set_compressor


def benchmark_compressors(ds, compressors_names, clevels, dst_dir="/tmp/", prefix="", suffix=""):
    benchmark_dict = {} 
    benchmark_dict["writing"] = {} 
    benchmark_dict["reading"] = {}
    benchmark_dict["filesize"] = {}
    for compressor_name in compressors_names:
        for clevel in clevels:
            if compressor_name == "blosc":
                algorithms = get_valid_blosc_algorithms()
            else:
                algorithms = [""]
    
            for algorithm in algorithms:
                # Define compressor kwargs
                if compressor_name == "blosc":
                    kwargs = {"clevel": clevel, "algorithm": algorithm}
                else:
                    kwargs = {"clevel": clevel}
                # Define compressor name 
                if algorithm == "":
                    compressor_acronym = f"{compressor_name}_c{clevel}"
                else:
                    compressor_acronym = f"{compressor_name}_{algorithm}_c{clevel}"
                if prefix != "": 
                    compressor_acronym = f"{prefix}_{compressor_acronym}"
                if suffix != "": 
                    compressor_acronym = f"{compressor_acronym}_{suffix}"
                # Define ZarrStore path
                store_path = os.path.join(dst_dir, f"example2_{compressor_acronym}.zarr.zip")
                print(compressor_acronym)
        
                # Set compressor
                compressor = get_compressor(compressor_name=compressor_name, **kwargs)
                compressor_dict = check_compressor(ds, compressor)
                ds = set_compressor(ds, compressor_dict)
                
                # Writing 
                t_i = time.time()
                zarr_store = zarr.ZipStore(store_path, mode="w")
                ds.to_zarr(store=zarr_store)
                zarr_store.close()
                t_f = time.time()
                benchmark_dict["writing"][compressor_acronym] = round(t_f - t_i, 1)
                
                # Measure file size 
                benchmark_dict["filesize"][compressor_acronym] = round(os.path.getsize(store_path) / (1024 ** 2), 2)
                
                # Reading 
                t_i = time.time()
                ds_read = xr.open_zarr(
                    store_path, chunks={}, decode_cf=True, mask_and_scale=True
                )
                ds_read = ds_read.compute()
                t_f = time.time()
                benchmark_dict["reading"][compressor_acronym] = round(t_f - t_i, 1)
                
    return benchmark_dict


def get_reading_time(fpath, isel_dict={}, n_repetitions=5):
    """Return the reading time of a Dataset (subset)."""

    def _load(fpath, isel_dict):
        ds = xr.open_zarr(fpath)
        ds = ds.isel(isel_dict)
        ds.load()

    times = []
    for i in range(1, n_repetitions):
        t_i = time.time()
        _load(fpath, isel_dict)
        times.append(time.time() - t_i)
    return times


def get_reading_throughput(fpath, isel_dict={}, n_repetitions=10):
    """Return the reading throughput (MB/s) of a Dataset (subset)."""
    times = get_reading_time(fpath=fpath, isel_dict=isel_dict, n_repetitions=n_repetitions)
    size_dict = get_memory_size_zarr(fpath, isel_dict=isel_dict)
    throughput = sum(size_dict.values()) / np.array(times)
    return throughput.tolist()


def get_writing_time(
    ds,
    fpath,
    chunks=None,  # Use current chunking
    compressor=None,
    consolidated=True,
    n_repetitions=5,
    remove_last=True,
):
    """Return the writing time of a Dataset."""
    times = []
    for i in range(1, n_repetitions):
        t_i = time.time()
        write_zarr(
            zarr_fpath=fpath,
            ds=ds,
            chunks=chunks,
            compressor=compressor,
            consolidated=consolidated,
            show_progress=False,
        )
        times.append(time.time() - t_i)
        if i < n_repetitions - 1:
            # Remove store
            shutil.rmtree(fpath)
    if remove_last:
        shutil.rmtree(fpath)
    return times


def get_writing_throughput(
    ds,
    fpath,
    chunks=None,  # Use current chunking
    compressor=None,
    consolidated=True,
    n_repetitions=5,
    remove_last=True,
):
    """Return the writing throughput (MB/s) of a Dataset."""
    times = get_writing_time(
        ds,
        fpath,
        chunks=chunks,
        compressor=compressor,
        consolidated=consolidated,
        n_repetitions=n_repetitions,
        remove_last=remove_last,
    )
    size_dict = get_memory_size_dataset(ds)
    throughput = sum(size_dict.values()) / np.array(times)
    return throughput.tolist()


def profile_zarr_io(
    ds,
    fpath,
    chunks=None,  # Use current chunking
    compressor=None,
    isel_dict={},
    consolidated=True,
    n_repetitions=5,
):
    """Profile reading and writing of a Dataset."""
    io_dict = {}
    io_dict["writing"] = get_writing_time(
        ds=ds,
        fpath=fpath,
        chunks=chunks,
        compressor=compressor,
        consolidated=consolidated,
        n_repetitions=n_repetitions,
        remove_last=False,
    )
    io_dict["reading"] = get_reading_time(
        fpath=fpath, isel_dict=isel_dict, n_repetitions=n_repetitions
    )
    io_dict["reading_throughput"] = get_reading_throughput(
        fpath=fpath, isel_dict=isel_dict, n_repetitions=n_repetitions
    )
    io_dict["compression_ratio"] = get_storage_ratio_zarr(fpath=fpath)
    shutil.rmtree(fpath)
    return io_dict


# -----------------------------------------------------------------------------.
#########################################
#### Define default zarr compressors ####
#########################################
def _get_blosc_compressors(clevels=[0, 1, 3, 5, 9]):
    """Get blosc compressors."""
    cnames = numcodecs.blosc.list_compressors()
    shuffles = [
        numcodecs.Blosc.BITSHUFFLE,
        numcodecs.Blosc.SHUFFLE,
        numcodecs.Blosc.NOSHUFFLE,
    ]
    possible_args = list(itertools.product(shuffles, clevels, cnames))
    compressors = {}
    for shuffle, clevel, cname in possible_args:
        k_name = cname + "_cl" + str(clevel) + "_s" + str(shuffle)
        compressors[k_name] = numcodecs.blosc.Blosc(cname=cname, clevel=clevel, shuffle=shuffle)
    return compressors


def _get_lmza_compressors(clevels=[0, 1, 3, 5, 9]):
    """Get lmza compressors."""
    # - preset: compression level between 0 and 9
    # - dist: distance between bytes to be subtracted (default 1)
    # Cannot specify filters except with FORMAT_RAW
    import lzma

    delta_dist = [None, 1, 2, 4]
    possible_args = list(itertools.product(clevels, delta_dist))
    compressors = {}
    for clevel, delta_dist in possible_args:
        if delta_dist is not None:
            lzma_filters = [
                dict(id=lzma.FILTER_DELTA, dist=delta_dist),
                dict(id=lzma.FILTER_LZMA2, preset=clevel),
            ]
            k_name = "LZMA" + "_cl" + str(clevel) + "_delta" + str(delta_dist)
            compressors[k_name] = numcodecs.LZMA(preset=None, filters=lzma_filters)
        else:
            k_name = "LZMA" + "_cl" + str(clevel) + "_nodelta"
            compressors[k_name] = numcodecs.LZMA(preset=clevel, filters=None)
    return compressors


def _get_zip_compressors(clevels=[0, 1, 3, 5, 9]):
    """Get zip compressor."""
    # - BZ2 do not accept clevel = 0
    compressors = {}
    for clevel in clevels:
        k_name = "GZip" + "_cl" + str(clevel)
        compressors[k_name] = numcodecs.gzip.GZip(level=clevel)
        if clevel > 0:
            k_name = "BZ2" + "_cl" + str(clevel)
            compressors[k_name] = numcodecs.bz2.BZ2(level=clevel)
    return compressors


def _get_zfpy_compressors():
    """Get zfpy compressor."""
    # TODO define some options
    # - Not yet available for Python 3.8.5
    # - precision: A integer number, specifying the compression precision needed
    compressors = {}
    compressors["zfpy"] = numcodecs.zfpy.ZFPY(tolerance=-1, rate=-1, precision=-1)
    return compressors


def _getlossless_compressors(clevels=[0, 1, 3, 5, 9]):
    """Get lossless compressors."""
    compressors = _get_blosc_compressors(clevels=clevels)
    # compressors.update(_get_lmza_compressors(clevels=clevels))
    compressors.update(_get_zip_compressors(clevels=clevels))
    # compressors.update(_get_zfpy_compressors())
    return compressors
