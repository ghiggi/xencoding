# Welcome to xencoding
[![DOI](https://zenodo.org/badge/664629093.svg)](https://zenodo.org/badge/latestdoi/664629093)
[![PyPI version](https://badge.fury.io/py/xencoding.svg)](https://badge.fury.io/py/xencoding)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/xencoding.svg)](https://anaconda.org/conda-forge/xencoding)
[![Tests](https://github.com/ghiggi/xencoding/actions/workflows/tests.yml/badge.svg)](https://github.com/ghiggi/xencoding/actions/workflows/tests.yml)
[![Coverage Status](https://coveralls.io/repos/github/ghiggi/xencoding/badge.svg?branch=main)](https://coveralls.io/github/ghiggi/xencoding?branch=main)
[![Documentation Status](https://readthedocs.org/projects/xencoding/badge/?version=latest)](https://xencoding.readthedocs.io/projects/xencoding/en/stable/?badge=stable)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/github/license/ghiggi/xencoding)](https://github.com/ghiggi/xencoding/blob/master/LICENSE)

The xencoding is still in development. Feel free to try it out and to report issues or to suggest changes.

## Quick start

xencoding provides an easy-to-use tool to define and optimize netCDF and Zarr Dataset encodings.

Look at the [Tutorials][tutorial_link] to have an overview of the software !

## Installation

### pip

xencoding can be installed via [pip][pip_link] on Linux, Mac, and Windows.
On Windows you can install [WinPython][winpy_link] to get Python and pip
running.
Then, install the xencoding package by typing the following command in the command terminal:

    pip install xencoding

To install the latest development version via pip, see the
[documentation][doc_install_link].

### conda [NOT YET AVAILABLE]

xencoding can be installed via [conda][conda_link] on Linux, Mac, and Windows.
Install the package by typing the following command in a command terminal:

    conda install xencoding

In case conda forge is not set up for your system yet, see the easy to follow
instructions on [conda forge][conda_forge_link].


## Documentation for xencoding

You can find the documentation under [xencoding.readthedocs.io][doc_link]

### Tutorials and Examples

The documentation also includes some [tutorials][tutorial_link], showing the most important use cases of xencoding.
These tutorial are also available as Jupyter Notebooks and in Google Colab:

- 1. Introduction to YAML file encoding specification [[Notebook][tut3_label_link]][[Colab][colab3_label_link]]
- 2. Introduction to encoding benchmarking [[Notebook][tut3_label_link]][[Colab][colab3_label_link]]


## Citation

If you are using xencoding in your publication please cite our paper:

You can cite the Zenodo code publication of xencoding by:

> Ghiggi Gionata & XXXX . ghiggi/xencoding. Zenodo. https://doi.org/10.5281/zenodo.8131552

If you want to cite a specific version, have a look at the [Zenodo site](https://doi.org/10.5281/zenodo.7753488).

## Requirements:

- [xarray](https://docs.xarray.dev/en/stable/)
- [netcdf4](hhttps://unidata.github.io/netcdf4-python/)
- [zarr](https://github.com/zarr-developers/zarr-python)
- [numcodecs](https://github.com/zarr-developers/numcodecs)

## License

The content of this repository is released under the terms of the [MIT](LICENSE) license.


[pip_link]: https://pypi.org/project/gstools
[conda_link]: https://docs.conda.io/en/latest/miniconda.html
[conda_forge_link]: https://github.com/conda-forge/xencoding-feedstock#installing-xencoding
[conda_pip]: https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-pkgs.html#installing-non-conda-packages
[pipiflag]: https://pip-python3.readthedocs.io/en/latest/reference/pip_install.html?highlight=i#cmdoption-i
[winpy_link]: https://winpython.github.io/

[tutorial_link]: https://github.com/ghiggi/xencoding/tree/master#tutorials-and-examples

[tut3_label_link]: https://github.com/ghiggi/xencoding/tree/master/tutorials
[colab3_label_link]: https://github.com/ghiggi/xencoding/tree/master/tutorials

[tut3_patch_link]: https://github.com/ghiggi/xencoding/tree/master/tutorials
[colab3_patch_link]: https://github.com/ghiggi/xencoding/tree/master/tutorials
