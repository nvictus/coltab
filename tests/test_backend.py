from __future__ import division, print_function
import tempfile
import os.path as op
import os

import numpy as np
import pandas as pd
import coltab
from coltab.backend import HDF5Store
import pytest


testdir = os.path.dirname(os.path.realpath(__file__))
tmp = tempfile.gettempdir()
EXAMPLE_PATH = os.path.join(tmp, 'test.h5')


@pytest.fixture(scope='module')
def store(request):
    
    def teardown():
        if op.exists(EXAMPLE_PATH):
            os.remove(EXAMPLE_PATH)
    request.addfinalizer(teardown)

    return HDF5Store(EXAMPLE_PATH)


def test_create_table(store):
    with store:
        tab = store.create_table(
            'baz', 
            {'A': int, 'B': int, 'C':float})

        assert 'baz' in store.grp.keys()


def test_append(store):
    in_df = pd.DataFrame({
        'A': [1,2,3], 
        'B': [4.1, 5.1, 6.1], 
        'C': [4.1, 5.1, 6.1]
    })
    with store:
        store.append('baz', in_df)
        out_df = store.select('baz')
    assert len(in_df) == len(out_df)
    assert np.all(in_df.columns == out_df.columns)


def test_append_col(store):
    with store:
        store.addcol('baz', 'E', [6,7,8])
        df = store.select('baz')
    assert 'E' in df.columns


def test_remove_col(store):
    with store:
        store.delcol('baz', 'D')
        df = store.select('baz')
    assert 'D' not in df.columns


def test_drop_table(store):
    with store:
        store.drop_table('baz')
        with pytest.raises(KeyError):
            store.select('baz')
