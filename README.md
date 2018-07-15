# Coltab
Pillars of data architecture

Take any directory-like mutable mapping for arrays, and view and manipulate it as a table (i.e. dataframe). That's it!

```python

>>> import coltab
>>> import h5py

# low-level API
>>> f = h5py.File('myfile.h5', 'r+')
>>> df = coltab.get(f['foo/bar'], fields=['a', 'b', 'c'], lo=10, hi=100)
>>> coltab.put(f['baz'].create_group('quux'), df, 
... 	storage_options={'compression': 'gzip', 'compression_opts': 6, 'shuffle': True})

# high-level API
>>> view = coltab.Table('h5py://myfile.h5::foo/bar', storage_options=**opts)  # dask dataframe?
>>> df = view.loc[10:100]
>>> df = view[:]
>>> coltab.to_table('h5py://myfile.h5::baz/quux', view)


>>> coltab.shuffle()
>>> coltab.sort()
>>> coltab.indexing.rlencode()
>>> coltab.indexing.sparse_index()


```
