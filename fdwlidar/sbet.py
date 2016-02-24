#!/usr/bin/env python
# -*- coding: utf-8 -*-
from multicorn import ForeignDataWrapper


class Sbet(ForeignDataWrapper):

    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns

    def execute(self, quals, columns):
        pass
