#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
from os import path
from pathlib import Path, PurePath
from struct import Struct
from multicorn import ForeignDataWrapper

root = Path(__file__).resolve().parent
raw = Path(PurePath(root, 'data/pulse-float32-phi/43724.bin')).resolve()
theta = Path(PurePath(root, 'data/pulse-float32-theta/43724.bin')).resolve()
time = Path(PurePath(root, 'data/pulse-linear-time/43724.txt')).resolve()


class EchoPulse(ForeignDataWrapper):

    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns

    def execute(self, quals, columns):
        yield from read_pulse()


def read_pulse():
    for r, tta, ti in zip(read_float(raw), read_float(theta), gen_time()):
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


def gen_time():
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
