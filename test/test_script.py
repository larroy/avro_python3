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

import csv
import io
import json
import logging
import operator
import os
import subprocess
import tempfile
import unittest

import avro.datafile
import avro.io
import avro.schema


# ------------------------------------------------------------------------------


NUM_RECORDS = 7

SCHEMA = """
{
  "namespace": "test.avro",
  "name": "LooneyTunes",
  "type": "record",
  "fields": [
    {"name": "first", "type": "string"},
    {"name": "last", "type": "string"},
    {"name": "type", "type": "string"}
  ]
}
"""

LOONIES = (
    ('daffy', 'duck', 'duck'),
    ('bugs', 'bunny', 'bunny'),
    ('tweety', '', 'bird'),
    ('road', 'runner', 'bird'),
    ('wile', 'e', 'coyote'),
    ('pepe', 'le pew', 'skunk'),
    ('foghorn', 'leghorn', 'rooster'),
)


def looney_records():
  for f, l, t in LOONIES:
    yield {'first': f, 'last' : l, 'type' : t}


SCRIPT = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'avro')

# The trailing spaces are expected when pretty-printing JSON with json.dumps():
_JSON_PRETTY = '\n'.join([
    '{',
    '    "first": "daffy", ',
    '    "last": "duck", ',
    '    "type": "duck"',
    '}',
])


# ------------------------------------------------------------------------------


class TestCat(unittest.TestCase):

  @staticmethod
  def WriteAvroFile(file_path):
    schema = avro.schema.Parse(SCHEMA)
    with open(file_path, 'wb') as writer:
      with avro.datafile.DataFileWriter(
          writer=writer,
          datum_writer=avro.io.DatumWriter(),
          writer_schema=schema,
      ) as writer:
        for record in looney_records():
          writer.append(record)

  def setUp(self):
    # TODO: flag to not delete the files
    delete = True
    self._avro_file = (
        tempfile.NamedTemporaryFile(prefix='test-', suffix='.avro', delete=delete))
    TestCat.WriteAvroFile(self._avro_file.name)

  def tearDown(self):
    self._avro_file.close()

  def _Run(self, *args, raw=False, **kw):
    command = [SCRIPT, 'cat', self._avro_file.name] + list(args)
    logging.debug('Running command:\n%s', ' \\\n\t'.join(command))
    out = subprocess.check_output(command)
    out = out.decode('utf-8')
    if raw:
      return out
    else:
      return out.splitlines()

  def testPrint(self):
    return len(self._Run()) == NUM_RECORDS

  def testFilter(self):
    return len(self._Run('--filter', "r['type']=='bird'")) == 2

  def testSkip(self):
    skip = 3
    return len(self._Run('--skip', str(skip))) == NUM_RECORDS - skip

  def testCsv(self):
    reader = csv.reader(io.StringIO(self._Run('-f', 'csv', raw=True)))
    self.assertEqual(len(list(reader)), NUM_RECORDS)

  def testCsvHeader(self):
    sio = io.StringIO(self._Run('-f', 'csv', '--header', raw=True))
    reader = csv.DictReader(sio)
    expected = {'type': 'duck', 'last': 'duck', 'first': 'daffy'}

    data = next(reader)
    self.assertEqual(expected, data)

  def testPrintSchema(self):
    out = self._Run('--print-schema', raw=True)
    self.assertEqual(json.loads(out)['namespace'], 'test.avro')

  def testHelp(self):
    # Just see we have these
    self._Run('-h')
    self._Run('--help')

  def testJsonPretty(self):
    out = self._Run('--format', 'json-pretty', '-n', '1', raw=1)
    self.assertEqual(
        out.strip(),
        _JSON_PRETTY.strip(),
        'Output mismatch\n'
        'Expect: %r\n'
        'Actual: %r'
        % (_JSON_PRETTY.strip(), out.strip()))

  def testVersion(self):
    subprocess.check_output([SCRIPT, 'cat', '--version'])

  def testFiles(self):
    out = self._Run(self._avro_file.name)
    self.assertEqual(len(out), 2 * NUM_RECORDS)

  def testFields(self):
    # One field selection (no comma)
    out = self._Run('--fields', 'last')
    self.assertEqual(json.loads(out[0]), {'last': 'duck'})

    # Field selection (with comma and space)
    out = self._Run('--fields', 'first, last')
    self.assertEqual(json.loads(out[0]), {'first': 'daffy', 'last': 'duck'})

    # Empty fields should get all
    out = self._Run('--fields', '')
    self.assertEqual(
        json.loads(out[0]),
        {'first': 'daffy', 'last': 'duck', 'type': 'duck'})

    # Non existing fields are ignored
    out = self._Run('--fields', 'first,last,age')
    self.assertEqual(
        json.loads(out[0]),
        {'first': 'daffy', 'last': 'duck'})


# ------------------------------------------------------------------------------


class TestWrite(unittest.TestCase):

  def setUp(self):
    delete = False

    self._json_file = tempfile.NamedTemporaryFile(
        prefix='test-', suffix='.json', delete=delete)
    with open(self._json_file.name, 'w') as f:
      for record in looney_records():
        json.dump(record, f)
        f.write('\n')

    self._csv_file = tempfile.NamedTemporaryFile(
        prefix='test-', suffix='.csv', delete=delete)
    with open(self._csv_file.name, 'w') as f:
      writer = csv.writer(f)
      get = operator.itemgetter('first', 'last', 'type')
      for record in looney_records():
        writer.writerow(get(record))

    self._schema_file = tempfile.NamedTemporaryFile(
        prefix='test-', suffix='.avsc', delete=delete)
    with open(self._schema_file.name, 'w') as f:
      f.write(SCHEMA)

  def tearDown(self):
    self._csv_file.close()
    self._json_file.close()
    self._schema_file.close()

  def _Run(self, *args, **kw):
    command = [SCRIPT, 'write', '--schema', self._schema_file.name] + list(args)
    logging.debug('Running command:\n%s', ' \\\n\t'.join(command))
    subprocess.check_call(command, **kw)

  def LoadAvro(self, filename):
    out = subprocess.check_output([SCRIPT, 'cat', filename])
    out = out.decode('utf-8')
    return tuple(map(json.loads, out.splitlines()))

  def testVersion(self):
    subprocess.check_call([SCRIPT, 'write', '--version'])

  def FormatCheck(self, format, filename):
    with tempfile.NamedTemporaryFile(prefix='test-', suffix='.dat') as temp:
      with open(temp.name, 'wb') as f:
        self._Run(filename, '-f', format, stdout=f)

      records = self.LoadAvro(temp.name)
      self.assertEqual(len(records), NUM_RECORDS)
      self.assertEqual(records[0]['first'], 'daffy')

  def testWriteJson(self):
    self.FormatCheck('json', self._json_file.name)

  def testWriteCsv(self):
    self.FormatCheck('csv', self._csv_file.name)

  def testOutfile(self):
    with tempfile.NamedTemporaryFile(prefix='test-', suffix='.dat') as temp:
      os.remove(temp.name)
      self._Run(self._json_file.name, '-o', temp.name)
      self.assertEqual(len(self.LoadAvro(temp.name)), NUM_RECORDS)

  def testMultiFile(self):
    with tempfile.NamedTemporaryFile(prefix='test-', suffix='.dat') as temp:
      with open(temp.name, 'wb') as f:
        self._Run(self._json_file.name, self._json_file.name, stdout=f)

      self.assertEqual(len(self.LoadAvro(temp.name)), 2 * NUM_RECORDS)

  def testStdin(self):
    with tempfile.NamedTemporaryFile(prefix='test-', suffix='.dat') as temp:
      with open(self._json_file.name, 'rb') as info:
        with open(temp.name, 'wb') as f:
          self._Run('--input-type', 'json', stdin=info, stdout=f)

      self.assertEqual(len(self.LoadAvro(temp.name)), NUM_RECORDS)


if __name__ == '__main__':
  raise Exception('Use run_tests.py')
