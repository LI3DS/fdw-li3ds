#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from binascii import unhexlify

import numpy as np
import pytest

from fdwli3ds import EchoPulse

data_dir = os.path.join(
    os.path.dirname(__file__), 'data', 'echopulse')


@pytest.fixture
def reader(scope='module'):
    ept = EchoPulse(
        options={
            'directory': data_dir,
            'pcid': '1'
        },
        columns=None
    )
    return ept


@pytest.fixture
def reader_offset(scope='module'):
    ept = EchoPulse(
        options={
            'directory': data_dir,
            'pcid': '1',
            'time_offset': '1300000'
        },
        columns=None
    )
    return ept


@pytest.fixture
def schema(scope='module'):
    ept = EchoPulse(
        options={
            'directory': data_dir,
            'metadata': 'true',
        },
        columns=None
    )
    return ept


def test_read_schema(schema):
    result = next(schema.execute(None, None))
    assert isinstance(result, dict)
    assert 'schema' in result
    assert len(result['schema']) > 0


def test_dimension_list(schema):
    assert [dim.name for dim in schema.dimensions] == [
        'x', 'y', 'z',
        'phi', 'reflectance', 'deviation', 'amplitude', 'num_echoes', 'echo'
    ]


def test_patch_size(reader):
    # get size for each dimension in metadata
    size_list = [
        int(dim.size)
        for dim in reader.dimensions
    ]
    # get first patch
    patch = next(reader.execute(None, None))
    # header of 5 bytes for each dimension
    # header of 13 bytes for the patch
    size = 13 + sum([5 + reader.patch_size * size for size in size_list])
    assert len(unhexlify(patch['points'])) == size


def test_point_count(reader):
    """
    All patch must have the correct number of points
    """
    allpatch = list(reader.execute(None, None))
    allpatch_size = sum([
        len(unhexlify(patch['points']))
        - 13  # remove header part
        - 5 * len(reader.dimensions)  # remove the 5 bytes for each dimensions
        for patch in allpatch
    ])
    point_size = sum(int(dim.size) for dim in reader.dimensions)
    assert int(allpatch_size / point_size) == 293679


def test_time_offset(reader_offset, reader):
    patch = next(reader.execute(None, None))
    patch_offset = next(reader_offset.execute(None, None))
    patch_nohead = unhexlify(patch['points'])[18:]
    patch_offset_nohead = unhexlify(patch_offset['points'])[18:]
    # read first dim (should be X -> time, hardcoded here)
    x_dim = [(dim.size, dim.type) for dim in reader.dimensions
             if dim.name == 'x'][0]
    values = np.fromstring(patch_nohead, dtype=x_dim[1], count=int(x_dim[0]))
    values_offset = np.fromstring(patch_offset_nohead, dtype=x_dim[1],
                                  count=int(x_dim[0]))
    assert float(values_offset[0] - values[0]) == 1300000
