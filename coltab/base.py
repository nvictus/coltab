"""
Two things:

* hierarchical kv-store
* array proxy

Input

* uri (resource_path + internal_path)
* storage mapper

Metadata could be stored in a table directory's attributes.

* column order
* table length

Datatypes

* numpy numeric
* pandas categorical
* numpy byte string S
* varlen unicode arrays
* unicode scalars

Attrs

* numpy scalar -> python native
* numpy array -> lists (possibly nested) of python natives
* unicode -> decode to str (can be further decoded as JSON or YAML to dict)

"""
from abc import abstractmethod


class Backend:
    """Base class for backends

    """
    @abstractmethod
    def open(self, mode):
        """Open the storage mapper. Should return a pathdict-like object"""
    
    @abstractmethod
    def __enter__(self):
        """Provide an open storage mapper context"""
    
    @abstractmethod
    def __exit__(self, *exc_info):
        """Cleanup any necessary resources for the storage mapper"""
    
    @property
    #@abstractmethod
    def attrs(self):
        """Return a pathdict that decodes values properly on accession"""
    
    @abstractmethod
    def create_table(self, name, colnames, dtypes, data=None):
        """Create a empty directory if one does not exist. 
        Initialize arrays for columns.
        """
    
    @abstractmethod
    def drop_table(self, name):
        """Remove the table's arrays and directory."""
    
    @abstractmethod
    def append(self, chunk):
        """Append columns from a dataframe to the columns in the table.
        All input columns must already exist in the table."""
    
    @abstractmethod
    def addcol(self, name, arr=None, dtype=None, pos=None):
        """Append a column to the table. 
        Must have the same length as the other columns."""
    
    @abstractmethod
    def delcol(self, name):
        """Remove a column from a table."""

    @abstractmethod
    def select(self, index, colnames=None):
        """Select a range of values from a table, and 
        optionally, a subset of columns.
        TODO: replace with .loc[]/.iloc[] or __getitem__?
        """


# class Table:

#     def __array__(self, ):
#         pass

#     def __getitem__(self, key):
#         pass

#     def __setitem__(self, key, value):
#         pass

#     def __len__(self):
#         pass

#     def __contains__(self, key):
#         pass

#     def get(self, key, default):
#         pass

#     def keys(self):
#         pass

#     def values(self):
#         pass

#     def items(self):
#         pass

#     @property
#     def columns(self):
#         pass

#     @property
#     def shape(self):
#         pass

#     @loc.getter
#     def _loc(self, key):
#         pass

#     @iloc.getter
#     def _iloc(self, key):
#         pass

#     def to_dict(self):
#         pass

#     def to_dask(self, partition):
#         pass

# from . import backend

# class _IndexingMixin(object):

#     def _unpack_index(self, key):
#         if isinstance(key, tuple):
#             if len(key) == 2:
#                 row, col = key
#             elif len(key) == 1:
#                 row, col = key[0], slice(None)
#             else:
#                 raise IndexError('invalid number of indices')
#         else:
#             row, col = key, slice(None)
#         return row, col

#     def _isintlike(self, num):
#         try:
#             int(num)
#         except (TypeError, ValueError):
#             return False
#         return True

#     def _process_slice(self, s, nmax):
#         if isinstance(s, slice):
#             if s.step not in (1, None):
#                 raise ValueError('slicing with step != 1 not supported')
#             i0, i1 = s.start, s.stop
#             if i0 is None:
#                 i0 = 0
#             elif i0 < 0:
#                 i0 = nmax + i0
#             if i1 is None:
#                 i1 = nmax
#             elif i1 < 0:
#                 i1 = nmax + i1
#             return i0, i1
#         elif self._isintlike(s):
#             if s < 0:
#                 s += nmax
#             if s >= nmax:
#                 raise IndexError('index is out of bounds')
#             return int(s), int(s + 1)
#         else:
#             raise TypeError('expected slice or scalar')


# class Table(_IndexingMixin):
    
#     def __init__(self, opener, uri, fields=None, get=backend.select, put=backend.put):
#         self.opener = opener
#         self.uri = uri
#         self._get = get
#         self._put = put
#         with opener(uri) as grp:
#             n = 0
#             for field in grp.keys():
#                 n = max(n, len(grp[field]))
#             self._shape = (n,)
#             if fields is None:
#                 self._fields = list(grp.keys())
#             else:
#                 self._fields = fields

#     def __array__(self):
#         return np.asarray(self[:])

#     @property
#     def shape(self):
#         return self._shape

#     @property
#     def columns(self):
#         with self.opener(self.uri) as grp:
#             return self._get(grp, self._fields, 0, 0).columns
    
#     def __getitem__(self, key):
#         # requesting a subset of columns
#         if isinstance(key, (list, str)):
#             return self.__class__(
#                 self.opener,
#                 self.uri,
#                 key,
#                 self._get,
#                 self._put)

#         # requesting an interval of rows
#         if isinstance(key, tuple):
#             if len(key) == 1:
#                 key = key[0]
#             else:
#                 raise IndexError('too many indices for table')
        
#         lo, hi = self._process_slice(key, self._shape[0])
#         with self.opener(self.uri) as grp:
#             return self._get(grp, self._fields, lo, hi)
            
#     def __setitem__(self, key, value):
#         lo, hi = self._process_slice(key, self._shape[0])
#         with self.opener(self.uri) as grp:
#             if hi is None or (hi - lo) == len(value):
#                 self._put(grp, df, lo)

#     def __len__(self):
#         return self._shape[0]

#     def __contains__(self, key):
#         pass

#     def keys(self):
#         return list(self.columns)

# #     def values(self):
# #         pass

# #     def items(self):
# #         pass

# #     @property
# #     def columns(self):
# #         pass

# #     @property
# #     def shape(self):
# #         pass

# #     @loc.getter
# #     def _loc(self, key):
# #         pass

# #     @iloc.getter
# #     def _iloc(self, key):
# #         pass

#     def to_dict(self):
#         with self.opener(self.uri) as grp:
#             return self._get(grp, as_dict=True)

#     def to_dask(self, partition):
#         pass
    
# #     def __repr__(self):
# #         return 
