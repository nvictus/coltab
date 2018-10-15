# Coltab

```python
>>> import coltab
>>> store = coltab.HDF5Store('/tmp/myfile.h5')
>>> store.open('a')
>>> store.create_table('foo', {'a': int, 'b': int, 'c': float})
>>> store.append('foo', {'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
>>> store.addcol('foo', 'd', [1.1, 2.2, 3.3])
>>> store.delcol('foo', 'c')
>>> store.select('foo')
   a  b    c    d
0  1  4  7.0  1.1
1  2  5  8.0  2.2
2  3  6  9.0  3.3
store.drop_table('foo')
store.close()
```

<!-- Pillars of data architecture

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
 -->