#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import struct
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


@pytest.fixture
def schema_with_mapping(scope='module'):
    ept = EchoPulse(
        options={
            'directory': data_dir,
            'metadata': 'true',
            'map_x': 'range',
            'map_time': 'the_time'
        },
        columns=None
    )
    return ept


def test_read_schema(schema):
    result = next(schema.execute(None, None))
    assert isinstance(result, dict)
    assert 'schema' in result
    assert len(result['schema']) > 0


def test_schema_structure(schema):
    result = next(schema.execute(None, None))
    pcschema = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), 'data/echopulse/pcschema.xml')
    assert isinstance(result, dict)
    assert 'schema' in result
    with open(pcschema) as xmlref:
        ref = xmlref.read()
        assert result['schema'] == ref


def test_schema_with_mapping(schema_with_mapping):
    assert 'the_time' in [dim.name for dim in schema_with_mapping.dimensions]
    assert 'float64' == [
        dim.type for dim in schema_with_mapping.dimensions
        if dim.name == 'the_time'
    ][0]


def test_dimension_list(schema):
    assert sorted([dim.name for dim in schema.dimensions]) == [
        'amplitude', 'deviation', 'echo',
        'n_echo', 'reflectance', 'time', 'x', 'y', 'z',
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
    print('toffset : ', reader_offset.time_offset)
    patch_nohead = unhexlify(patch['points'])
    patch_offset_nohead = unhexlify(patch_offset['points'])
    times = extract_dimension(patch_nohead, reader.dimensions, 'time')
    times_offset = extract_dimension(patch_offset_nohead, reader_offset.dimensions, 'time')
    assert float(times_offset[0] - times[0]) == 1300000


def extract_dimension(patch, dimensions, name):
    '''
    Extract a dimension in a patch with dimensional compression.
    Returns a numpy array
    '''
    # compute the offset needed to find the dimension
    # first offset is for the patch header
    offset = 13
    for dim in dimensions:
        # skip dimensional type on 1b
        print('type: ', struct.unpack('<b', patch[offset]))
        dimsize = int(struct.unpack('<I', patch[offset + 1: offset + 5])[0])
        if dim.name == name:
            # dimension found!
            break
        offset += 5 + dimsize
        print(dim.name, dim.size, dimsize, offset)
    return np.fromstring(patch[offset + 5:offset + 5 + dimsize], dtype=dim.type)
