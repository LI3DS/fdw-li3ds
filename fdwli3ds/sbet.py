#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math
from pathlib import Path
from glob import glob
from struct import pack
from binascii import hexlify

import numpy as np
from multicorn.utils import log_to_postgres

from .foreignpc import ForeignPcBase


class Sbet(ForeignPcBase):
    """
    Main class for sbet file format reading.
    Sbet stands for Smoothed Best Estimate of Trajectory.
    Sbet is the output format of POSPac post processing.
    POSPac is a popular Applanix program that post processes GPS and inertial data.

    Options:

        - sources: file glob pattern for source files (ex: *.sbet)
        - patch_size: how many points sewing in a patch
    """  # NOQA

    def __init__(self, options, columns):
        super(Sbet, self).__init__(options, columns)

        if 'sources' in options:
            self.sources = [
                Path(source).resolve()
                for source in glob(options['sources'])
            ]
            log_to_postgres('{} sbet file(s) linked'.format(len(self.sources)))
        # set default patch size to 100 points if not given
        self.patch_size = int(options.get('patch_size', 100))
        # sbet schema is provided
        self.pcschema = Path(__file__).parent / 'schemas' / 'sbetschema.xml'

    def execute(self, quals, columns):
        # When the metadata parameter has been passed to the foreign table
        # creation, we send metadata instead of data itself
        # This way we will be able to implement IMPORT FOREIGN SCHEMA for
        # both tables ( data / metadata ) at the same time
        if self.metadata:
            yield {'schema': self.read_pcschema()}
            return

        for source in self.sources:
            yield from self.read_sbet(source)

    def read_sbet(self, sbetfile):
        """
        Read a sbet file and yield patches.

        Patch binary structure:

            patch binary structure for WKB encoding
            byte:         endianness (1 = NDR, 0 = XDR)
            uint32:       pcid (key to POINTCLOUD_SCHEMAS)
            uint32:       0 = no compression
            uint32:       npoints
            pointdata[]:  interpret relative to pcid
            header = pack('<b3I', 1, pcid, 0, patch_size)
        """

        # get scaling factors for x, y, z coordinates
        scale_x = float([
            dim.scale for dim in self.dimensions
            if dim.name.lower() == 'x'][0])
        scale_y = float([
            dim.scale for dim in self.dimensions
            if dim.name.lower() == 'y'][0])
        scale_z = float([
            dim.scale for dim in self.dimensions
            if dim.name.lower() == 'z'][0])

        # apply conversion from radian to degrees for x, y only
        rad2deg_scaled_x = 180 / math.pi / scale_x
        rad2deg_scaled_y = 180 / math.pi / scale_y

        # store numpy structured types
        sbet_source_type = [(dim.name, 'double') for dim in self.dimensions]
        sbet_patch_type = [(dim.name, dim.type) for dim in self.dimensions]

        # open file as a memory map in Copy-on-write mode
        # (assignments affect data in memory, but changes are not saved to
        # disk. The file on disk is read-only)
        sbet = np.memmap(str(sbetfile), dtype=sbet_source_type, mode='c')
        sbet_size = len(sbet)
        # constructs slices according to patch_size
        slices = [
            slice(a, b)
            for a, b in zip(
                range(0, sbet_size, self.patch_size),
                range(self.patch_size, sbet_size, self.patch_size)
            )
        ]
        if slices[-1].stop != sbet_size:
            # append the end of the array
            slices.append(slice(slices[-1].stop, sbet_size))

        for sli in slices:
            subarray = sbet[sli]
            # convert to degrees and apply scale factor
            subarray['x'] = rad2deg_scaled_x * subarray['x']
            subarray['y'] = rad2deg_scaled_y * subarray['y']
            subarray['z'] = subarray['z'] / scale_z
            subarray['m_time'] += self.time_offset
            # cast to pointcloud xml schema types
            subarray = subarray.astype(sbet_patch_type)
            header = pack('<b3I', 1, self.pcid, 0, sli.stop - sli.start)
            yield {'points': hexlify(header + subarray.tostring())}
