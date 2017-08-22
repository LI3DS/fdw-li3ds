import struct

import numpy as np


def strtobool(v):
    return v.lower() in ('yes', 'true', 't', '1')


def extract_dimension(patch, dimensions, name, compression=None):
    '''
    Extract a dimension in a patch with dimensional compression.
    Returns a numpy array
    '''
    if not compression:
        dtype = [(dim.name, dim.type) for dim in dimensions]
        # convert to degrees and apply scale factor
        return np.fromstring(patch[13:], dtype=dtype)

    if compression == 'dimensional':
        dim_header = 5
        # compute the offset needed to find the dimension
        # first offset is for the patch header
        offset = 13
        for dim in dimensions:
            # skip dimensional type on 1b
            dimsize = int(struct.unpack('<I', patch[offset + 1: offset + 5])[0])
            if dim.name == name:
                # dimension found!
                break
            offset += dim_header + dimsize
        return np.fromstring(
            patch[offset + dim_header:offset + dim_header + dimsize],
            dtype=dim.type)
