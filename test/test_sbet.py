#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from binascii import unhexlify

import pytest

from fdwli3ds import Sbet
from fdwli3ds.util import extract_dimension

sbet_file = os.path.join(
    os.path.dirname(__file__), 'data', 'sbet', 'sbet.bin')


@pytest.fixture
def reader(scope='module'):
    ept = Sbet(
        options={
            'sources': sbet_file,
            'pcid': '1'
        },
        columns=None
    )
    return ept


@pytest.fixture
def reader_offset(scope='module'):
    ept = Sbet(
        options={
            'sources': sbet_file,
            'pcid': '1',
            'time_offset': '1300000'
        },
        columns=None
    )
    return ept


@pytest.fixture
def reader_overlap(scope='module'):
    ept = Sbet(
        options={
            'sources': sbet_file,
            'pcid': '1',
            'overlap': 'true'
        },
        columns=None
    )
    return ept


@pytest.fixture
def schema(scope='module'):
    ept = Sbet(
        options={
            'sources': sbet_file,
            'metadata': 'true'
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
        'm_time',
        'y',
        'x',
        'z',
        'm_xVelocity',
        'm_yVelocity',
        'm_zVelocity',
        'm_roll',
        'm_pitch',
        'm_plateformHeading',
        'm_wanderAngle',
        'm_xAcceleration',
        'm_yAcceleration',
        'm_zAcceleration',
        'm_xBodyAngularRate',
        'm_yBodyAngularRate',
        'm_zBodyAngularRate'
    ]


def test_patch_size(reader):
    # get first patch
    patch = next(reader.execute(None, None))
    size_list = [
        int(dim.size)
        for dim in reader.dimensions
    ]
    # 13 bytes for header
    size = 13 + sum([reader.patch_size * size for size in size_list])
    assert len(unhexlify(patch['points'])) == size


def test_point_count(reader):
    """
    All patch must have the correct number of points (5000 in the test file)
    """
    reader.overlap = False
    allpatch = list(reader.execute(None, None))
    # header size for each patch
    header_size = 13
    allpatch_size = sum([
        len(unhexlify(patch['points'])) - header_size
        for patch in allpatch
    ])
    point_size = sum(int(dim.size) for dim in reader.dimensions)
    assert allpatch_size / point_size == 50000


def test_time_offset(reader_offset, reader):
    patch = next(reader.execute(None, None))
    patch_offset = next(reader_offset.execute(None, None))
    patch_nohead = unhexlify(patch['points'])
    patch_offset_nohead = unhexlify(patch_offset['points'])
    times = extract_dimension(
        patch_nohead,
        reader.dimensions,
        'm_time')
    times_offset = extract_dimension(
        patch_offset_nohead,
        reader_offset.dimensions,
        'm_time')
    assert times_offset[0] - times[0] == 1300000


def test_nonoverlap_patch(reader):
    reader.overlap = False
    read = reader.execute(None, None)
    first_patch = unhexlify(next(read)['points'])
    second_patch = unhexlify(next(read)['points'])
    first_array = extract_dimension(first_patch, reader.dimensions, 'm_time')
    second_array = extract_dimension(second_patch, reader.dimensions, 'm_time')
    assert first_array[-1] != second_array[0]


def test_overlap_patch(reader_overlap):
    read = reader_overlap.execute(None, None)
    first_patch = unhexlify(next(read)['points'])
    second_patch = unhexlify(next(read)['points'])
    first_array = extract_dimension(first_patch, reader_overlap.dimensions, 'm_time')
    second_array = extract_dimension(second_patch, reader_overlap.dimensions, 'm_time')
    assert first_array[-1] == second_array[0]
