#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from pathlib import Path
from struct import pack
from collections import defaultdict
import xml.etree.ElementTree as etree
from binascii import hexlify

import numpy as np
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

# pattern for the echo/pulse schema directory
subtree_pattern = re.compile(r'^(echo|pulse)-([\w\d]+)-(.*)$')

# since pgpointcloud requires x and y dimensions we don't have them
# on raw data so we map the couple (x,y) to some raw dimensions
pcschema2raw = {
    'x': 'time',
    'y': 'theta',
    'z': 'range'
}


class EchoPulse(ForeignDataWrapper):
    """
    Foreign class for the Echo/Pulse/Table format
    """
    def __init__(self, options, columns):
        """
        Initialize with options passed through the create foreign table
        statement
        """
        super().__init__(options, columns)
        self.columns = columns
        # Resolve data files found in directory
        self.source_dirs = [
            source.resolve()
            for source in Path(options['directory']).iterdir()
            if source.is_dir() and subtree_pattern.match(source.name)
        ]
        log_to_postgres('{} echo/pulse directories linked'
                        .format(len(self.source_dirs)))
        # set default patch size to 100 points if not given
        self.patch_size = int(options.get('patch_size', 400))
        # pcschema.xml must be present in the directory
        self.pcschema = Path(options['directory']) / 'pcschema.xml'
        # pcid used to create WKB patchs
        self.pcid = int(options.get('pcid', 0))
        # next option is used to retrieve pcschema.xml back to postgres
        self.metadata = options.get('metadata', False)
        # get time offset if provided
        self.time_offset = float(options.get('time_offset', 0))

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

    @property
    def dimensions(self):
        """
        List all dimensions from the pcschema xml description
        """
        root = etree.fromstring(self.read_pcschema())
        return [
            elem.text
            for elem in root.iter('{http://pointcloud.org/schemas/PC/1.1}name')
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
        """
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
        self.raw_dimensions = [pcschema2raw.get(dim, dim) for dim in self.dimensions]

        for sdir in self.source_dirs:
            filelist = [sfi for sfi in sdir.glob('*')]
            # ordered by name (which is in fact time)
            filelist.sort()
            source_files_count.add(len(filelist))
            # extracting informations on data types and signal types
            signal, datatype, name = subtree_pattern.match(sdir.name).groups()

            # contruct a tuple to store all informations needed to read the data
            directories.append((sdir.name, signal, datatype, name, filelist))

        # sort on signal and datatype
        directories.sort(
            key=lambda x: (x[1], x[2] == 'linear', x[3] == 'num_echoes'),
            reverse=True
        )

        # check consistency, sub directories must have the same number of files
        if len(source_files_count) != 1:
            raise Exception('Consistency failed, bad number of files in source directories')

        framelist = []

        for idx in range(source_files_count.pop()):
            framelist.append(defaultdict(dict))
            for sdir, signal, datatype, name, filelist in directories:
                framelist[-1][signal][(datatype, name)] = filelist[idx]

        # start reading and creating patches
        yield from self.generate_patch(framelist)

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

        """
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
            # slicing over dimensions
            for subarray in slices:
                buff = [
                    pack('<bI', 0, values.nbytes) +  # header for each dimension
                    values.tostring()  # data content
                    for _, att in att_array
                    for values in [att[subarray]]
                ]
                header = pack('<b3I', 1, self.pcid, 2, subarray.stop - subarray.start)
                yield {'points': hexlify(header + b''.join(buff))}

    def read_ept(self, frame):
        # read first linear time and pop it
        pulses = frame['pulse']
        timefile = pulses.pop(('linear', 'time'))
        with timefile.open('r') as tfile:
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
        pulsetime = pulse_arrays['time'] = (
            np.ones(nentries, dtype='float64') * t0
            + np.arange(nentries, dtype='float64') * delta
        )

        echo_arrays = {}
        echos = frame['echo']

        vec_echo = pulse_arrays['num_echoes']
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

        # Duplicate all items in pulse arrays according to num_echoes number
        # We must create a copy of num_echoes array with zero values replaced by 1
        # in order to repeat correctly items without deleting zero items
        num_echoes_copy = pulse_arrays['num_echoes'].copy()

        # remove zero value in order to use the repeat function without deleting rows
        num_echoes_copy[num_echoes_copy == 0] = 1

        # duplicate rows having more than 1 echoe
        for name, p in pulse_arrays.items():
            pulse_arrays[name] = pulse_arrays[name].repeat(num_echoes_copy)

        pulse_arrays.update(echo_arrays)
        # return ordered arrays according to xml schema
        return sorted(
            pulse_arrays.items(),
            key=lambda x: self.raw_dimensions.index(x[0])
        )
