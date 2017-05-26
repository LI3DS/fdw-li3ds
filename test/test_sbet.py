#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from binascii import unhexlify

import pytest

from fdwli3ds import Sbet

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
            'timeoffset': '1300000'
        },
        columns=None
    )
    return ept


@pytest.fixture
def schema(scope='module'):
    ept = Sbet(
        options={
            'sources': sbet_file,
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
    allpatch = list(reader.execute(None, None))
    # header size for each patch
    header_size = 13
    allpatch_size = sum([
        len(unhexlify(patch['points'])) - header_size
        for patch in allpatch
    ])
    point_size = sum(int(dim.size) for dim in reader.dimensions)
    assert allpatch_size / point_size == 50000
