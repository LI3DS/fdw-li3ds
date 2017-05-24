#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import namedtuple
import xml.etree.ElementTree as etree

from multicorn import ForeignDataWrapper


# used to store dimension details
dimension = namedtuple('dimensions', ['name', 'size', 'type', 'scale'])
# Xml namespace
PC_NAMESPACE = '{http://pointcloud.org/schemas/PC/1.1}'


class ForeignPcBase(ForeignDataWrapper):
    """
    Foreign PointCloud Base class
    """

    def __init__(self, options, columns):
        """
        Initialize with options passed through the create foreign table
        statement
        """
        super(ForeignPcBase, self).__init__(options, columns)
        self.columns = columns
        # set default patch size to 100 points if not given
        self.patch_size = int(options.get('patch_size', 400))
        # pcid used to create WKB patchs
        self.pcid = int(options.get('pcid', 0))
        # next option is used to retrieve pcschema.xml back to postgres
        self.metadata = options.get('metadata', False)
        # get time offset if provided
        self.time_offset = float(options.get('time_offset', 0))
        # will store dimension infos
        self._dimensions = None

    def read_pcschema(self):
        """
        Read pointcloud XML schema and returns its content.
        The schema document format used by PostgreSQL Pointcloud is the same
        one used by the PDAL library.
        """
        content = ''
        with open(self.pcschema) as f:
            content = f.read()
        return content

    @property
    def dimensions(self):
        """
        Get dimensions detail from the pcschema xml description
        """
        if self._dimensions:
            return self._dimensions

        root = etree.fromstring(self.read_pcschema())
        self._dimensions = [
            (int(elem.findtext('{}position'.format(PC_NAMESPACE))),
                dimension(
                elem.findtext('{}name'.format(PC_NAMESPACE)),
                elem.findtext('{}size'.format(PC_NAMESPACE)),
                elem.findtext('{}interpretation'.format(PC_NAMESPACE)),
                elem.findtext('{}scale'.format(PC_NAMESPACE)) or 1,
            ))
            for elem in root.iter('{}dimension'.format(PC_NAMESPACE))
        ]
        # reorder and remove position
        self._dimensions = [
            dim for _, dim in sorted(self._dimensions, key=lambda dim: dim[0])
        ]
        return self._dimensions
