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
   a  b    d
0  1  4  1.1
1  2  5  2.2
2  3  6  3.3
store.drop_table('foo')
store.close()
```
