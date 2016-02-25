#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math
from pathlib import Path, PurePath
from struct import Struct, pack

from multicorn import ForeignDataWrapper

root = Path(__file__).resolve().parent
sbetfile = Path(PurePath(root, 'data/sbet.bin')).resolve()


class Sbet(ForeignDataWrapper):

    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns

    def execute(self, quals, columns):
        yield from read_sbet()


def read_sbet():
    # scale factor to convert radians to degrees and shifts the 7 decimal digits
    # to encode in uint32
    rad2deg_scaled = 180*1e7 / math.pi

    # list of rows to insert into database
    rows = []

    # number of points in a patch
    PATCH_SIZE = 100

    # sbet structure
    item = Struct('<17d')
    item_size = 17*8
    unp = item.unpack

    # patch binary structure for WKB encoding
    # byte:         endianness (1 = NDR, 0 = XDR)
    # uint32:       pcid (key to POINTCLOUD_SCHEMAS)
    # uint32:       0 = no compression
    # uint32:       npoints
    # pointdata[]:  interpret relative to pcid
    header = pack('<b3I', 1, 1, 0, PATCH_SIZE)

    # initialize a patch structure
    point_struct = Struct('<dIII13d')
    pack_point = point_struct.pack

    # number of points read
    npoints = 0
    # staging points in WKB format
    points = []

    with sbetfile.open('rb') as sbet:
        while True:
            data = sbet.read(item_size)
            if not data:
                break
            point = list(unp(data))

            point[1] = int(point[1] * rad2deg_scaled)
            point[2] = int(point[2] * rad2deg_scaled)
            point[3] = int(point[3] / 0.01)
            points.append(pack_point(*point))

            npoints += 1

            if npoints % PATCH_SIZE == 0 and npoints:
                # insert a new patch
                hexa = (header + b''.join(points)).hex()
                points = []
                yield {
                    'patch': hexa
                }

    # treat points left
    if points:
        header = pack('<b3I', *[1, 1, 0, len(points)])
        hexa = (header + b''.join(points)).hex()

    yield {
        'patch': hexa
    }
