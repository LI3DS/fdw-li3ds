#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math
from pathlib import Path
from glob import glob
from struct import Struct, pack
from binascii import hexlify

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres


class Sbet(ForeignDataWrapper):
    """
    Main class for sbet file format reading.
    Sbet stands for Smoothed Best Estimate of Trajectory.
    Sbet is the output format of POSPac post processing.
    POSPac is a popular Applanix program that post processes GPS and inertial data.

    option list available for the foreign table:

        - sources: file glob pattern for source files (ex: *.sbet)
        - patch_size: how many points sewing in a patch
    """
    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns
        self.sources = [
            Path(source).resolve()
            for source in glob(options['sources'])
        ]
        log_to_postgres('{} sbet file(s) linked'.format(len(self.sources)))
        # set default patch size to 100 points if not given
        self.patch_size = int(options.get('patch_size', 100))
        # pcschema.xml must be present in the directory
        self.pcschema = Path(options['sources']) / 'pcschema.xml'
        # pcid used to create WKB patchs
        self.pcid = int(options.get('pcid', 0))
        # next option is used to retrieve pcschema.xml back to postgres
        self.metadata = options.get('metadata', False)

    def read_pcschema(self):
        """
        Read pointcloud XML schema and returns its content.
        The schema document format used by PostgreSQL Pointcloud is the same one
        used by the PDAL library.
        """
        content = ''
        with self.pcschema.open() as f:
            content = f.read()
        return content

    def execute(self, quals, columns):
        if self.metadata:
            yield {'schema': self.read_pcschema()}
            return

        for source in self.sources:
            yield from read_sbet(source, self.patch_size, self.pcid)


def read_sbet(sbetfile, patch_size, pcid):
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
    header = pack('<b3I', 1, pcid, 0, patch_size)

    # initialize a patch structure
    point_struct = Struct('<dIII13d')
    pack_point = point_struct.pack

    # number of points read
    npoints = 0
    # staging points in WKB format
    points = []
    pappend = points.append

    with sbetfile.open('rb') as sbet:
        while True:
            data = sbet.read(item_size)
            if not data:
                break
            point = list(unp(data))

            point[1] = int(point[1] * rad2deg_scaled)
            point[2] = int(point[2] * rad2deg_scaled)
            point[3] = int(point[3] / 0.01)
            pappend(pack_point(*point))

            npoints += 1

            if npoints % patch_size == 0 and npoints:
                # insert a new patch
                hexa = hexlify(header + b''.join(points))
                points.clear()
                yield {'points': hexa}

    # treat points left
    if points:
        header = pack('<b3I', *[1, 1, 0, len(points)])
        hexa = hexlify(header + b''.join(points))

    yield {'points': hexa}
