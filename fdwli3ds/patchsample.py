#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math
import random
import time
from binascii import hexlify
from struct import Struct, pack
from multicorn import ForeignDataWrapper


class PatchSample(ForeignDataWrapper):
    """PatchSample is a PostgreSQL multicorn foreign data wrapper
    generating a grid of pgPointCloud PCPatch rows.
    Options :
        - npx : number of patches on x
        - npy : number of patches on y
        - nppp : number of point per patch
        - space : distance between two points in a patch
    """

    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns
        self.npx = int(options['npx'])
        self.npy = int(options['npy'])
        self.nppp = int(options['nppp'])
        self.space = float(options['space'])

    def execute(self, quals, columns):
        yield from gen_patches(self.npx, self.npy, self.nppp, self.space)


def gen_patches(npx, npy, nppp, space):

    # PCPatch structure
    #
    # patch binary structure for WKB encoding
    # byte:         endianness (1 = NDR, 0 = XDR)
    # uint32:       pcid (key to POINTCLOUD_SCHEMAS)
    # uint32:       0 = no compression
    # uint32:       npoints
    # pointdata[]:  interpret relative to pcid
    #
    # TODO : the pcid should be taken from column definition
    header = pack('<b3I', 1, 1, 0, nppp)

    # initialize a patch structure according to our pc schema of pcid 1
    # We have time, x, y, z, and a random value as double
    point_struct = Struct('<ddddd')
    pack_point = point_struct.pack

    # number of points per dimension of patch ( same for x and y)
    pppsqrt = int(math.floor(math.sqrt(nppp)))
    # We want npx * npy patches
    for i in range(npx):
        for j in range(npy):
            # points of the patch in wkb
            # nppp : number of points per patch
            points = []
            for k in range(pppsqrt):
                for l in range(pppsqrt):
                    points.append(
                        pack_point(
                            time.time(),
                            i * pppsqrt * space + k * space,
                            j * pppsqrt * space + l * space,
                            0.0,
                            random.random()))
            hexa = hexlify(header + b''.join(points))
            yield {
                'points': hexa
            }

if __name__ == '__main__':
    # Test py calling the script from interpreter
    p = PatchSample({'npx': 10, 'npy': 10, 'nppp': 100, 'space': 1}, {})
    print(list(p.execute([], {})))
