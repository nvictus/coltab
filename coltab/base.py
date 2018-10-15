"""
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
