# -*- coding: utf-8 -*-
from __future__ import division, print_function
from pandas.api.types import is_categorical  # is_categorical_dtype
import numpy as np
import pandas as pd
import h5py
import zarr
import six

from .base import Backend


class StorageError(Exception):
    pass


def resource_split(uri):
    try:
        uri, data_path = uri.rsplit("::", 1)
    except ValueError:
        data_path = ""
    try:
        proto, uri = uri.split("://", 1)
    except ValueError:
        proto = ""
    return proto, uri, data_path


class HierarchicalArrayStore(Backend):
    @property
    def is_open(self):
        return self._open

    @property
    def storer(self):
        if self._open:
            return self.grp
        else:
            raise StorageError("Store is closed.")

    def __enter__(self):
        self.open(**self.open_kws)
        return self

    def __exit__(self, *exc_info):
        self.close()

    def append(self, name, chunk):
        if not self._open:
            raise ValueError("Operation on closed file")
        assert set(chunk.keys()) == set(self.grp[name].keys())
        first = list(self.grp[name].keys())[0]
        self._put(name, chunk, len(self.grp[name][first]))

    def addcol(self, name, colname, arr, dtype=None, pos=None):
        """Append a column to the table.
        Must have the same length as the other columns."""
        if not self._open:
            raise ValueError("Operation on closed file")
        d = {colname: arr}
        self._put(name, d, 0)

    def delcol(self, name, colname):
        """Remove a column from a table."""
        if not self._open:
            raise ValueError("Operation on closed file")
        if colname in self.grp[name].keys():
            del self.grp[name][colname]

    def select(self, name, lo=0, hi=None, colnames=None):
        """Select a range of values from a table, and
        optionally, a subset of columns.
        TODO: replace with .loc[]/.iloc[] or __getitem__?
        """
        if not self._open:
            raise ValueError("Operation on closed file")
        return self._get(name, lo, hi, colnames)

    def tree(self):
        if self._open:
            import zarr.util

            return zarr.util.TreeViewer(self.grp)
        else:
            raise ValueError("Operation on closed file")

    def _normalize_column(self, data, coerce_dtype=None, store_categories=True):
        """
        Make column suitable for HDF5 storage.

        * numerical and boolean types map as they should
        * bytes colunms map to type S arrays -- they won't roundtrip
        * str or object columns map to type S arrays
        * categorical columns:
            * make an ENUM type (may end up being too large for HDF5 to accept)

        """
        if coerce_dtype is not None:
            coerce_dtype = np.dtype(coerce_dtype)

        if np.isscalar(data):
            array = np.array([data], dtype=coerce_dtype)
            dtype = data.dtype
            fillvalue = None

        elif is_categorical(data):
            if store_categories:
                cats = data.cat.categories
                enum_dict = dict(zip(cats, range(len(cats))))
                array = data.cat.codes
                dtype = h5py.special_dtype(enum=(array.dtype, enum_dict))
                fillvalue = -1
            else:
                array = data.cat.codes
                dtype = coerce_dtype or array.dtype
                fillvalue = -1

        elif data.dtype in (object, str, bytes):
            data = np.asarray(data)
            dtype = np.dtype("S")
            array = np.array(data, dtype=dtype)
            fillvalue = None

        else:
            array = np.asarray(data)
            dtype = data.dtype
            fillvalue = None

        return array, dtype, fillvalue

    def _put(self, name, chunk, row_offset):
        """Append columns from a dataframe to the columns in the table.
        All input columns must already exist in the table."""
        grp = self.grp[name]
        lo = row_offset
        if isinstance(chunk, pd.Series):
            chunk = chunk.to_frame()
            n_rows = len(chunk)
        else:
            n_rows = len(chunk[next(iter(chunk.keys()))])
        hi = lo + n_rows

        for name in chunk.keys():

            x = np.asarray(chunk[name])

            data, dtype, fillvalue = self._normalize_column(x, x.dtype)

            if name in grp.keys():
                dset = grp[name]
                if hi > len(dset):
                    dset.resize((hi,))
                dset[lo:hi] = data
            else:
                try:
                    enum_dict = h5py.check_dtype(enum=dtype)
                except AttributeError:
                    enum_dict = None
                dset = grp.create_dataset(
                    name,
                    shape=(hi,),
                    dtype=dtype,
                    data=data,
                    fillvalue=fillvalue,
                    **self.storage_options
                )
                if enum_dict is not None:
                    # store enum dictionary as attribute
                    dset.attrs["categories"] = sorted(
                        enum_dict, key=enum_dict.__getitem__
                    )

    def _get(self, name, lo=0, hi=None, colnames=None, decode_enum=True, as_dict=False):
        grp = self.grp[name]
        series = False
        if colnames is None:
            colnames = list(grp.keys())
        elif isinstance(colnames, six.string_types):
            colnames = [colnames]
            series = True

        data = {}
        for name in colnames:
            dset = grp[name]

            categories = None
            if decode_enum and "categories" in dset.attrs:
                categories = dset.attrs["categories"].astype("U")

            if categories is not None:
                data[name] = pd.Categorical.from_codes(
                    dset[lo:hi], categories, ordered=True
                )
            elif dset.dtype.type == np.string_:
                data[name] = dset[lo:hi].astype("U")
            else:
                # vlen_dtype = h5py.check_dtype(vlen=dset.dtype)
                # no need to do this, vlen arrays automatically come out as object
                data[name] = dset[lo:hi]

        if as_dict:
            return data

        if data and lo is not None:
            index = np.arange(lo, lo + len(next(iter(data.values()))))
        else:
            index = None

        if series:
            return pd.Series(data[colnames[0]], index=index, name=colnames[0])
        else:
            return pd.DataFrame(data, columns=colnames, index=index)


class HDF5Store(HierarchicalArrayStore):

    DEFAULT_STORAGE_OPTS = {
        "compression": "gzip",
        "compression_opts": 6,
        "shuffle": True,
    }
    ARRAY_TYPE = h5py.Dataset
    GROUP_TYPE = h5py.Group

    def __init__(self, store_uri, storage_options=None, **kwargs):
        """
        Parameters
        ----------
        store_uri : str
            URI to HDF5 file or group.
        storage_options : dict
            ``h5py.Dataset`` storage options.
        **kwargs
            Keyword args to pass ``h5py.open`` when used as a context manager.
            Default is to open in append 'a' mode.

        """
        _, store_path, group_path = resource_split(store_uri)
        self.store_path = store_path
        self.group_path = group_path or "/"
        self.storage_options = storage_options or self.DEFAULT_STORAGE_OPTS
        self.open_kws = kwargs or {"mode": "a"}
        self._open = False

    def open(self, mode):
        """Opens the file. Idempotent."""
        if not self._open:
            self.grp = h5py.File(self.store_path, mode)[self.group_path]
            self._open = True

    def close(self):
        """Closes the file. Idempotent."""
        if self._open:
            self.grp.file.close()
            self._open = False

    def create_table(self, name, dtypes=None, data=None, init_size=0, max_size=None):
        """Create a empty group if one does not exist.
        Initialize arrays for columns.
        """
        if not self._open:
            raise ValueError("Operation on closed file")

        if name in self.grp and isinstance(self.grp[name], self.ARRAY_TYPE):
            raise StorageError(
                "Cannot create group '{}'. Dataset already exists.".format(name)
            )

        grp = self.grp.require_group(name)

        if len(grp.keys()):
            raise StorageError("Existing group '{}' is not empty.".format(name))

        if dtypes is not None:
            for colname in dtypes.keys():
                grp.create_dataset(
                    colname,
                    shape=(init_size,),
                    maxshape=(max_size,),
                    dtype=dtypes[colname],
                    **self.storage_options
                )
        elif data is not None:
            for colname in data.keys():
                self.addcol(name, colname, data[colname])

    def drop_table(self, name):
        """Remove the table's arrays and group."""
        if not self._open:
            raise ValueError("Operation on closed file")

        grp = self.grp[name]
        if isinstance(grp, self.GROUP_TYPE) and all(
            isinstance(k, self.ARRAY_TYPE) for k in grp.values()
        ):
            table_name = grp.name
            if table_name == "/":
                for colname in self.grp.keys():
                    self.delcol(grp, colname)
            else:
                parent = grp.parent
                del parent[name]


class ZarrStore(HierarchicalArrayStore):

    DEFAULT_STORAGE_OPTS = {}
    ARRAY_TYPE = zarr.Array
    GROUP_TYPE = zarr.Group

    def __init__(self, store_uri, consolidated=False, storage_options=None, **kwargs):
        _, store_path, group_path = resource_split(store_uri)
        self.store_path = store_path
        self.group_path = group_path
        self.storage_options = storage_options or self.DEFAULT_STORAGE_OPTS
        self.open_kws = kwargs
        self._open = False

    def open(self, mode):
        """Opens the store. Idempotent."""
        if not self._open:
            self.grp = zarr.open_group(self.store_path, mode)[self.group_path]
            self._open = True

    def close(self):
        """Closes the store. Idempotent."""
        if self._open:
            self._open = False

    def create_table(self, name, dtypes=None, data=None, init_size=0):
        """Create a empty group if one does not exist.
        Initialize arrays for columns.
        """
        if not self._open:
            raise ValueError("Operation on closed store")

        if name in self.grp and isinstance(self.grp[name], self.ARRAY_TYPE):
            raise StorageError(
                "Cannot create group '{}'. Array already exists.".format(name)
            )

        grp = self.grp.require_group(name)

        if len(grp.keys()):
            raise StorageError("Existing group '{}' is not empty.".format(name))

        if dtypes is not None:
            for colname in dtypes.keys():
                grp.create_dataset(
                    colname,
                    shape=(init_size,),
                    dtype=dtypes[colname],
                    **self.storage_options
                )
        elif data is not None:
            for colname in data.keys():
                self.addcol(name, colname, data[colname])

    def drop_table(self, name):
        """Remove the table's arrays and group."""
        if not self._open:
            raise ValueError("Operation on closed store")

        table_grp = self.grp[name]
        if isinstance(table_grp, self.GROUP_TYPE) and all(
            isinstance(k, self.ARRAY_TYPE) for k in table_grp.values()
        ):
            table_name = table_grp.name
            if table_name == "/":
                for colname in self.grp.keys():
                    self.delcol(table_grp, colname)
            else:
                del self.grp[name]
