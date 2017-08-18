#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
import glob
from struct import pack
from collections import defaultdict, namedtuple
from binascii import hexlify
from StringIO import StringIO

import numpy as np
from multicorn.utils import log_to_postgres

from .foreignpc import ForeignPcBase

# pattern for the echo/pulse schema directory
subtree_pattern = re.compile(r'^(echo|pulse)-([\w\d]+)-(.*)$')

# used to store dimension details
dimension = namedtuple('dimensions', ['name', 'size', 'type', 'scale'])

schema_skeleton = """<?xml version="1.0" encoding="UTF-8"?>
<pc:PointCloudSchema xmlns:pc="http://pointcloud.org/schemas/PC/1.1"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
{dimensions}
<pc:metadata>
    <Metadata name="compression">dimensional</Metadata>
</pc:metadata>
</pc:PointCloudSchema>
"""

xml_dimension = """<pc:dimension>
    <pc:position>{}</pc:position>
    <pc:size>{}</pc:size>
    <pc:name>{}</pc:name>
    <pc:description></pc:description>
    <pc:interpretation>{}</pc:interpretation>
</pc:dimension>"""


TYPE_MAPPER = {
    'linear': 'double',
}


def get_types(intype):
    return TYPE_MAPPER.get(intype, intype)


def get_size(strtype):
    """
    returns size in bytes from a string like float32
    """
    if strtype == 'linear':
        # linear time is always a double
        return 8
    return int(re.search(r'\d+', strtype).group()) / 8


class EchoPulse(ForeignPcBase):
    """
    Foreign class for the Echo/Pulse/Table format
    """

    def __init__(self, options, columns):
        """
        Initialize with options passed through the create foreign table
        statement
        """
        super(EchoPulse, self).__init__(options, columns)
        # Resolve data files found in directory
        self.basedir = options['directory']
        sources = (source for source in os.listdir(self.basedir)
                   if subtree_pattern.match(source))
        self.source_dirs = [
            os.path.realpath(os.path.join(self.basedir, source))
            for source in sources
            if os.path.isdir(os.path.join(self.basedir, source))
        ]
        # default mapping for coordinates
        self.new_dimnames = {
            'range': 'x',
            'theta': 'y',
            'phi': 'z'
        }
        # get custom mapping given in options
        varmapping = [
            opt for opt in options.keys()
            if opt.startswith('map_')
        ]
        for var in varmapping:
            self.new_dimnames.update({var.strip('map_'): options[var]})

        # get pointcloud structure from the directory tree
        self.ordered_dims = self.scan_structure()

        log_to_postgres('{} echo/pulse directories linked'
                        .format(len(self.source_dirs)))

    @property
    def pcschema(self):
        xml = schema_skeleton.format(
            dimensions='\n'.join([
                xml_dimension.format(
                    idx, size,
                    self.new_dimnames.get(name, name), dtype)
                for idx, size, name, dtype in self.ordered_dims
            ])
        )
        return StringIO(xml)

    def scan_structure(self):
        """
        Scan directory structure and generate the appropriate
        pointcloud schema.
        One directory corresponds to one dimension.
        Dimensions are always ordered by alphabetical order for idempotence
        Returns a tuple like that:
            [
                (1, 32, 'phi', 'float32'),
                (2, 8, 'n_echo', 'uint8'),
                ...
            ]
        """
        dimensions = []
        for subdir in os.listdir(self.basedir):
            if not subdir.startswith('echo') and \
               not subdir.startswith('pulse'):
                # dimension directory should start with the signal type
                continue
            _, dtype, name = subdir.split('-')
            dimensions.append((
                get_size(dtype),
                name,
                get_types(dtype)))

        # add the echo index (computed in the code above)
        dimensions.append(('1', 'echo', 'int8'))
        sorted_dims = sorted(dimensions, key=lambda x: x[1])
        return [
            (idx, dim[0], dim[1], dim[2])
            for idx, dim in enumerate(sorted_dims, start=1)
        ]

    def execute(self, quals, columns):
        """
        Called each time a request is made on the foreign table.
        Yields each row as a mapping of column: value

        All directories corresponding to an attribute are scanned first, then
        all files are ordered by time (one file defines one second of acquisition).
        A dataframe is composed of a data file for each attribute.

        Here is an example of the dataframe structure:
        [
            {
                'pulse':
                    {('linear', 'time'): '1.txt', ('float32', 'phi'): '1.bin', },
                'echo':
                    {('float32', 'amplitude'): '1.txt', ('float32', 'range'): '1.bin', },
            },
            {
                'pulse':
                    {('linear', 'time'): '2.txt', ('float32', 'phi'): '2.bin', },
                'echo':
                    {('float32', 'amplitude'): '2.txt', ('float32', 'range'): '2.bin', },
            },
        ]
        """  # NOQA
        if self.metadata:
            yield {'schema': self.read_pcschema()}
            return

        if len(self.source_dirs) < 8:
            log_to_postgres(
                'Nothing to return, there must be at least '
                '8 subdirectories for echo pulse data')
            return

        source_files_count = set()
        directories = []
        self.raw_dimensions = [name for s, _, name, _ in self.ordered_dims]

        for sdir in self.source_dirs:
            filelist = [sfi for sfi in glob.glob(os.path.join(sdir, '*'))]
            # ordered by name (which is in fact time)
            filelist.sort()
            source_files_count.add(len(filelist))
            # extracting informations on data types and signal types
            basename = os.path.basename(sdir)
            signal, datatype, name = subtree_pattern.match(basename).groups()

            # contruct a tuple to store all informations needed to read
            # the data
            directories.append((basename, signal, datatype, name, filelist))

        # sort on signal and datatype
        directories.sort(
            key=lambda x: (x[1], x[2] == 'linear', x[3] == 'n_echo'),
            reverse=True
        )

        # check consistency, sub directories must have the same number of files
        if len(source_files_count) != 1:
            raise Exception('Consistency failed, bad number of files in '
                            'source directories')

        framelist = []

        for idx in range(source_files_count.pop()):
            framelist.append(defaultdict(dict))
            for sdir, signal, datatype, name, filelist in directories:
                framelist[-1][signal][(datatype, name)] = filelist[idx]

        # start reading and creating patches
        for patch in self.generate_patch(framelist):
            yield patch

    def generate_patch(self, framelist):
        """
        Using dimensional compression since datasource is already arranged by dimension
        # byte:          endianness (1 = NDR, 0 = XDR)
        # uint32:        pcid (key to POINTCLOUD_SCHEMAS)
        # uint32:        2 = dimensional compression
        # uint32:        npoints
        # dimensions[]:  dimensionally compressed data for each dimension

        + one header for each dimension

        # byte:           dimensional compression type (0-3)
        # uint32:         size of the compressed dimension in bytes
        # data[]:         the compressed dimensional values

        """  # NOQA
        for idx, frame in enumerate(framelist):
            # read frame
            att_array = self.read_ept(frame)
            att_size = len(att_array[0][1])

            # generating slices for accessing subarrays
            # [slice(0, 100),
            #  slice(100, 200),
            #  slice(200, 300)...]
            slices = [
                slice(a, b)
                for a, b in zip(
                    range(0, att_size, self.patch_size),
                    range(self.patch_size, att_size, self.patch_size)
                )
            ]
            if slices[-1].stop != att_size:
                # append the end of the array
                slices.append(slice(slices[-1].stop, att_size))

            for sli in slices:
                buff = [
                    pack('<bI', 0, values.nbytes) +  # header for each dim
                    values.tostring()  # data content
                    for _, att in att_array
                    for values in [att[sli]]
                ]
                header = pack('<b3I', 1, self.pcid, 2, sli.stop - sli.start)
                yield {'points': hexlify(header + b''.join(buff))}

    def read_ept(self, frame):
        # read first linear time and pop it
        pulses = frame['pulse']
        timefile = pulses.pop(('linear', 'time'))
        with open(timefile, 'r') as tfile:
            nentries, _, t0, _, delta, _ = tfile.readline().split()
            nentries = int(nentries)
            t0 = float(t0) + self.time_offset
            delta = float(delta)

        # we have to read nentries in pulse files, let's go
        # initialize pulse array for this file
        pulse_arrays = {}
        for (datatype, name), filename in pulses.items():
            values = np.fromfile(str(filename), dtype=datatype, count=nentries)
            pulse_arrays[name] = values

        # compute time values and reference it
        pulse_arrays['time'] = (
            np.ones(nentries, dtype='float64') * t0 +
            np.arange(nentries, dtype='float64') * delta
        )

        echo_arrays = {}
        echos = frame['echo']

        vec_echo = pulse_arrays['n_echo']
        nechos = np.sum(vec_echo)

        # get zero indices and scale to indices in echo space !
        # astype needed -> https://github.com/numpy/numpy/issues/6198
        zero_indices = vec_echo.cumsum()[vec_echo == 0].astype('int64')

        for (datatype, name), filename in echos.items():
            values = np.fromfile(str(filename), dtype=datatype, count=nechos)
            echo_arrays[name] = np.insert(values, zero_indices, 0)

        # add the echo index as a new dimension
        echo_arrays['echo'] = np.fromiter([
            idx
            for ne in vec_echo
            for idx in range(ne)],
            dtype='uint8')
        # apply zero insert
        echo_arrays['echo'] = np.insert(
            echo_arrays['echo'], zero_indices, 0).astype('uint8')

        # Duplicate all items in pulse arrays according to n_echo number
        # We must create a copy of n_echo array with zero values replaced
        # by 1 in order to repeat correctly items without deleting zero items
        n_echo_copy = pulse_arrays['n_echo'].copy()

        # remove zero value in order to use the repeat function without
        # deleting rows
        n_echo_copy[n_echo_copy == 0] = 1

        # duplicate rows having more than 1 echoe
        for name, p in pulse_arrays.items():
            pulse_arrays[name] = pulse_arrays[name].repeat(n_echo_copy)

        pulse_arrays.update(echo_arrays)
        # return ordered arrays according to xml schema
        return sorted(
            pulse_arrays.items(),
            key=lambda x: self.raw_dimensions.index(x[0])
        )
