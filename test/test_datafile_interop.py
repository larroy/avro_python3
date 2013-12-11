#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import unittest

from avro import io
from avro import datafile

from gen_interop_data import DATUM

class TestDataFileInterop(unittest.TestCase):
  def testInterop(self):
    for f in ['py.avro']:
      logging.debug('Reading %s', f)

      # read data in binary from file
      reader = open(f, 'rb')
      datum_reader = io.DatumReader()
      dfr = datafile.DataFileReader(reader, datum_reader)
      for datum in dfr:
        self.assertIsNotNone(datum)
        self.assertEqual(datum, DATUM)


if __name__ == '__main__':
  raise Exception('Use run_tests.py')
