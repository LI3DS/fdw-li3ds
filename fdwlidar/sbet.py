#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math
from pathlib import Path, PurePath
from struct import Struct, pack
from binascii import hexlify

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres


class Sbet(ForeignDataWrapper):

    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns
        root = Path(__file__).resolve().parent
        self.sbetfile = Path(PurePath(root, options['filename'])).resolve()
        # set default patch size to 100 points if not given
        self.patch_size = int(options.get('patch_size', 100))
        self.metadata = bool(options.get('metadata', False))
        self.sbetschema = Path(PurePath(root, 'schemas', 'sbetschema.xml')).resolve()

    def execute(self, quals, columns):
        # When the metadata parameter has been passed to the foreign table
        # creation, we send metadata instead of data itself
        # This way we will be able to implement IMPORT FOREIGN SCHEMA for
        # both tables ( data / metadata ) at the same time
        if self.metadata is True:
            schema = ''
            with self.sbetschema.open() as f:
                schema = f.read()
            yield 4326, schema
        else:
            yield from read_sbet(self.sbetfile, self.patch_size)

def read_sbet(sbetfile, patch_size):
    # scale factor to convert radians to degrees and shifts the 7 decimal digits
    # to encode in uint32
    rad2deg_scaled = 180*1e7 / math.pi

    # list of rows to insert into database
    rows = []

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
    header = pack('<b3I', 1, 1, 0, patch_size)

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

            if npoints % patch_size == 0 and npoints:
                # insert a new patch
                hexa = hexlify(header + b''.join(points))
                points = []
                yield {
                    'points': hexa
                }

    # treat points left
    if points:
        header = pack('<b3I', *[1, 1, 0, len(points)])
        hexa = hexlify(header + b''.join(points))

    yield {
        'points': hexa
    }
