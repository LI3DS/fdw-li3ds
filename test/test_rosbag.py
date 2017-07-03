#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import pytest
from binascii import unhexlify

from fdwli3ds import Rosbag

data_dir = os.path.join(
    os.path.dirname(__file__), 'data', 'rosbag')

bagfile = 'session8_section0_1492648601948956966_0.bag'


@pytest.fixture
def reader_laser(scope='module'):
    rb = Rosbag(
        options={
            'rosbag': os.path.join(data_dir, bagfile),
            'topic': '/Laser/velodyne_points'
        },
        columns=None
    )
    return rb


@pytest.fixture
def reader_laser_max_count(scope='module'):
    rb = Rosbag(
        options={
            'rosbag': os.path.join(data_dir, bagfile),
            'topic': '/Laser/velodyne_points',
            'patch_count_pointcloud': 40
        },
        columns=None
    )
    return rb


@pytest.fixture
def schema_laser(scope='module'):
    rb = Rosbag(
        options={
            'rosbag': os.path.join(data_dir, bagfile),
            'topic': '/Laser/velodyne_points',
            'metadata': 'true'
        },
        columns=None
    )
    return rb


def test_laser_read_schema(schema_laser):
    result = next(schema_laser.execute(None, None))
    assert isinstance(result, dict)
    assert 'schema' in result
    assert '<pc:dimension>' in result['schema']
    assert len(result['schema']) > 0


def test_laser_read_patch(reader_laser):
    result = next(reader_laser.execute([], ('topic', 'time', 'patch', 'ply')))
    assert isinstance(result, dict)
    assert len(result) == 4
    assert 'topic' in result
    assert 'time' in result
    assert 'patch' in result
    assert 'ply' in result


def test_laser_patch_size(reader_laser_max_count):
    result = next(reader_laser_max_count.execute([], ('patch', )))
    # point size: 32 bytes
    # patch header size: 13 bytes
    patch_size = 13 + reader_laser_max_count.patch_count_pointcloud * 32
    assert len(unhexlify(result['patch'])) == patch_size
