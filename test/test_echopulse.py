#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pathlib import Path
from binascii import unhexlify
import xml.etree.ElementTree as etree

import pytest

from fdwpointcloud import EchoPulse

data_dir = Path(__file__).parent / 'data' / 'echopulse'


@pytest.fixture
def data(scope='module'):
    ept = EchoPulse(
        options={
            'directory': data_dir,
            'pcid': '1'
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
    assert schema.dimensions == [
        'x', 'y', 'z', 'phi',
        'reflectance', 'deviation', 'amplitude', 'num_echoes', 'echo'
    ]


def test_patch_size(schema, data):
    schema = next(schema.execute(None, None))
    root = etree.fromstring(schema['schema'])
    # get size for each dimension in metadata
    size_list = [
        int(elem.text)
        for elem in root.iter('{http://pointcloud.org/schemas/PC/1.1}size')
    ]
    # get fisst patch
    patch = next(data.execute(None, None))
    # 5 bytes for each dimension header
    # 13 bytes for the patch header
    size = 13 + sum([5 + data.patch_size * size for size in size_list])
    assert len(unhexlify(patch['points'])) == size
