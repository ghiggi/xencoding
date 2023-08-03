#!/usr/bin/env python3
"""
Created on Thu Aug  3 09:01:42 2023

@author: ghiggi
"""
import numcodecs


def _get_blosc(algorithm="lz4", clevel=1, shuffle=1, blocksize=0):
    """Get BLOSC compressor.

    algorithm=‘zstd’, ‘blosclz’, ‘lz4’, ‘lz4hc’, ‘zlib’ or ‘snappy’.
    NOSHUFFLE (0), SHUFFLE (1), BITSHUFFLE (2) or AUTOSHUFFLE (-1).
    """
    return numcodecs.blosc.Blosc(
        cname=algorithm,
        clevel=clevel,
        shuffle=shuffle,
        blocksize=blocksize,
    )


def _get_bz2(clevel=1):
    """Get BZ2 compressor."""
    if clevel == 0:
        print("BZ2 does not accept clevel=0")  # TODO: raise warning
        return None
    return numcodecs.bz2.BZ2(level=clevel)


def _get_gzip(clevel=1):
    """Get GZIP compressor."""
    return numcodecs.gzip.GZip(level=clevel)


def _get_zfpy_mode(mode):
    import zfpy

    if mode == "fixed_accuracy":
        return zfpy.mode_fixed_accuracy
    else:
        raise NotImplementedError()
    return mode


def _get_zfpy(mode="fixed_accuracy", tolerance=-1, rate=-1, precision=-1):
    """Get zfpy compressor.

    tolerance : double, optional
        A double-precision number, specifying the compression accuracy needed.
    rate : double, optional
        A double-precision number, specifying the compression rate needed.
    precision : int, optional
        A integer number, specifying the compression precision needed.
    """
    mode = _get_zfpy_mode(mode)
    return numcodecs.zfpy.ZFPY(mode, tolerance=tolerance, rate=rate, precision=precision)


def _get_zstd(clevel=1):
    """Get Zstd compressor.

    clevel from 1 to 21.
    """
    return numcodecs.blosc.zstd.Zstd(level=clevel)


def _get_zlib(clevel=1):
    return numcodecs.zlib.Zlib(level=clevel)


def _get_lz4(acceleration=1):
    """Get LZ4 compressor.

    The larger the acceleration value, the faster the algorithm, but also
        the lesser the compression.
    More info at: https://github.com/lz4/lz4
    """
    return numcodecs.lz4.LZ4(acceleration=acceleration)


def _get_lmza_filter(filter):
    """Get lmza filter."""
    import lzma

    if filter == "delta":
        filter = lzma.FILTER_DELTA
    elif filter == "lmza2":
        filter = lzma.FILTER_LZMA2
    else:
        filter = None
    return filter


def _get_lmza_filters_dict(filters, delta_dist, clevel):
    if isinstance(filters, type(None)):
        return None

    if isinstance(filters, str):
        filters = [filters]
    filters_dict = {filter: _get_lmza_filter(filter) for filter in filters if filter is not None}

    lmza_filters = []
    for name, filter in filters_dict.items():
        if name == "delta" and delta_dist is not None:
            lmza_filters.append(dict(id=filter, dist=delta_dist))
        elif name == "lmza2":
            lmza_filters.append(dict(id=filter, preset=clevel))
        else:
            pass
    if len(lmza_filters) == 0:
        lmza_filters = None
    return lmza_filters


def _get_lmza(clevel=1, filters=["delta", "lmza2"], delta_dist=None):
    """Get lmza compressors."""
    # - preset: compression level between 0 and 9
    # - dist: distance between bytes to be subtracted (default 1)
    # Cannot specify filters except with FORMAT_RAW
    if clevel == 0:
        clevel = None
    filters = _get_lmza_filters_dict(filters, delta_dist, clevel)
    return numcodecs.LZMA(preset=None, filters=filters)


def get_valid_compressors():
    compressors = ["blosc", "b2", "gzip", "zfpy", "zstd", "zlib", "lz4", "lmza"]
    return compressors


def check_compressor_name(compressor_name):
    valid_compressors = get_valid_compressors()
    if compressor_name not in valid_compressors:
        raise ValueError(f"Invalid compressor name. Valid names are {valid_compressors}")


def get_compressor(compressor_name, **kwargs):
    check_compressor_name(compressor_name)
    if compressor_name == "blosc":
        return _get_blosc(**kwargs)
    elif compressor_name == "b2":
        return _get_bz2(**kwargs)
    elif compressor_name == "gzip":
        return _get_gzip(**kwargs)
    elif compressor_name == "zfpy":
        return _get_zfpy(**kwargs)
    elif compressor_name == "zstd":
        return _get_zstd(**kwargs)
    elif compressor_name == "zlib":
        return _get_zlib(**kwargs)
    elif compressor_name == "lz4":
        return _get_lz4(**kwargs)
    elif compressor_name == "lmza":
        return _get_lmza(**kwargs)
    else:
        raise NotImplementedError()
