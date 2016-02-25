#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
from pathlib import Path, PurePath
from struct import Struct
from multicorn import ForeignDataWrapper

class EchoPulse(ForeignDataWrapper):
    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns
        root = Path(__file__).resolve().parent
        # Get file locations from FDW options
        self.raw = Path(PurePath(root, options['raw'])).resolve()
        self.theta = Path(PurePath(root, options['theta'])).resolve()
        self.time = Path(PurePath(root, options['time'])).resolve()

    def execute(self, quals, columns):
        yield from read_pulse(self.raw, self.theta, self.time)

def read_pulse(raw, theta, time):
    for r, tta, ti in zip(read_float(raw), read_float(theta), gen_time(time)):
        yield {'r': r, 'theta': tta, 'time': ti}


def read_float(filename):
    pulse_phi = Struct('<f')
    punp = pulse_phi.unpack
    with filename.open('rb') as infile:
        while True:
            value = infile.read(4)
            if not value:
                break
            yield punp(value)[0]


def gen_time(time):
    with time.open() as infile:
        line = infile.readline()
    nelem, tstart, delta = [elem.strip() for elem in line.split('|')]
    nelem = int(nelem)
    tstart = float(tstart)
    delta = float(delta)
    for i in range(nelem):
        yield (tstart + delta * i)


if __name__ == '__main__':
    # debug
    print(list(read_pulse())[:2])
