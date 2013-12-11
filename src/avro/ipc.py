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

"""RPC/IPC support."""

import abc
import http.client
import io

from avro import io as avro_io
from avro import protocol
from avro import schema

# ------------------------------------------------------------------------------
# Constants

HANDSHAKE_REQUEST_SCHEMA = schema.Parse("""
{
    "type": "record",
    "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
    "fields": [
        {"name": "clientHash",
         "type": {"type": "fixed", "name": "MD5", "size": 16}},
        {"name": "clientProtocol", "type": ["null", "string"]},
        {"name": "serverHash", "type": "MD5"},
    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
    ]
}
""")

HANDSHAKE_RESPONSE_SCHEMA = schema.Parse("""
{
    "type": "record",
    "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
    "fields": [
        {"name": "match",
         "type": {"type": "enum", "name": "HandshakeMatch",
                  "symbols": ["BOTH", "CLIENT", "NONE"]}},
        {"name": "serverProtocol",
         "type": ["null", "string"]},
        {"name": "serverHash",
         "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
    {"name": "meta",
         "type": ["null", {"type": "map", "values": "bytes"}]}
    ]
}
""")

HANDSHAKE_REQUESTOR_WRITER = avro_io.DatumWriter(HANDSHAKE_REQUEST_SCHEMA)
HANDSHAKE_REQUESTOR_READER = avro_io.DatumReader(HANDSHAKE_RESPONSE_SCHEMA)
HANDSHAKE_RESPONDER_WRITER = avro_io.DatumWriter(HANDSHAKE_RESPONSE_SCHEMA)
HANDSHAKE_RESPONDER_READER = avro_io.DatumReader(HANDSHAKE_REQUEST_SCHEMA)

META_SCHEMA = schema.Parse('{"type": "map", "values": "bytes"}')
META_WRITER = avro_io.DatumWriter(META_SCHEMA)
META_READER = avro_io.DatumReader(META_SCHEMA)

SYSTEM_ERROR_SCHEMA = schema.Parse('["string"]')

# protocol cache
REMOTE_HASHES = {}
REMOTE_PROTOCOLS = {}

# Decoder/encoder for a 32 bits big-endian integer.
UINT32_BE = avro_io.STRUCT_INT

# Default size of the buffers use to frame messages:
BUFFER_SIZE = 8192


# ------------------------------------------------------------------------------
# Exceptions


class AvroRemoteException(schema.AvroException):
  """
  Raised when an error message is sent by an Avro requestor or responder.
  """
  def __init__(self, fail_msg=None):
    schema.AvroException.__init__(self, fail_msg)

class ConnectionClosedException(schema.AvroException):
  pass


# ------------------------------------------------------------------------------
# Base IPC Classes (Requestor/Responder)


class BaseRequestor(object):
  """Base class for the client side of a protocol interaction."""
  def __init__(self, local_protocol, transceiver):
    self._local_protocol = local_protocol
    self._transceiver = transceiver
    self._remote_protocol = None
    self._remote_hash = None
    self._send_protocol = None

  @property
  def local_protocol(self):
    return self._local_protocol

  @property
  def transceiver(self):
    return self._transceiver

  # read/write properties
  def set_remote_protocol(self, new_remote_protocol):
    self._remote_protocol = new_remote_protocol
    REMOTE_PROTOCOLS[self.transceiver.remote_name] = self.remote_protocol
  remote_protocol = property(lambda self: self._remote_protocol,
                             set_remote_protocol)

  def set_remote_hash(self, new_remote_hash):
    self._remote_hash = new_remote_hash
    REMOTE_HASHES[self.transceiver.remote_name] = self.remote_hash
  remote_hash = property(lambda self: self._remote_hash, set_remote_hash)

  def set_send_protocol(self, new_send_protocol):
    self._send_protocol = new_send_protocol
  send_protocol = property(lambda self: self._send_protocol, set_send_protocol)

  def request(self, message_name, request_datum):
    """Writes a request message and reads a response or error message.

    Args:
      message_name: Name of the IPC method.
      request_datum: IPC request.
    Returns:
      The IPC response.
    """
    # build handshake and call request
    buffer_writer = io.BytesIO()
    buffer_encoder = avro_io.BinaryEncoder(buffer_writer)
    self.write_handshake_request(buffer_encoder)
    self.write_call_request(message_name, request_datum, buffer_encoder)

    # send the handshake and call request; block until call response
    call_request = buffer_writer.getvalue()
    return self.issue_request(call_request, message_name, request_datum)

  def write_handshake_request(self, encoder):
    local_hash = self.local_protocol.md5
    remote_name = self.transceiver.remote_name
    remote_hash = REMOTE_HASHES.get(remote_name)
    if remote_hash is None:
      remote_hash = local_hash
      self.remote_protocol = self.local_protocol
    request_datum = {}
    request_datum['clientHash'] = local_hash
    request_datum['serverHash'] = remote_hash
    if self.send_protocol:
      request_datum['clientProtocol'] = str(self.local_protocol)
    HANDSHAKE_REQUESTOR_WRITER.write(request_datum, encoder)

  def write_call_request(self, message_name, request_datum, encoder):
    """
    The format of a call request is:
      * request metadata, a map with values of type bytes
      * the message name, an Avro string, followed by
      * the message parameters. Parameters are serialized according to
        the message's request declaration.
    """
    # request metadata (not yet implemented)
    request_metadata = {}
    META_WRITER.write(request_metadata, encoder)

    # message name
    message = self.local_protocol.messages.get(message_name)
    if message is None:
      raise schema.AvroException('Unknown message: %s' % message_name)
    encoder.write_utf8(message.name)

    # message parameters
    self.write_request(message.request, request_datum, encoder)

  def write_request(self, request_schema, request_datum, encoder):
    datum_writer = avro_io.DatumWriter(request_schema)
    datum_writer.write(request_datum, encoder)

  def read_handshake_response(self, decoder):
    handshake_response = HANDSHAKE_REQUESTOR_READER.read(decoder)
    match = handshake_response.get('match')
    if match == 'BOTH':
      self.send_protocol = False
      return True
    elif match == 'CLIENT':
      if self.send_protocol:
        raise schema.AvroException('Handshake failure.')
      self.remote_protocol = protocol.Parse(
                             handshake_response.get('serverProtocol'))
      self.remote_hash = handshake_response.get('serverHash')
      self.send_protocol = False
      return True
    elif match == 'NONE':
      if self.send_protocol:
        raise schema.AvroException('Handshake failure.')
      self.remote_protocol = protocol.Parse(
                             handshake_response.get('serverProtocol'))
      self.remote_hash = handshake_response.get('serverHash')
      self.send_protocol = True
      return False
    else:
      raise schema.AvroException('Unexpected match: %s' % match)

  def read_call_response(self, message_name, decoder):
    """
    The format of a call response is:
      * response metadata, a map with values of type bytes
      * a one-byte error flag boolean, followed by either:
        o if the error flag is false,
          the message response, serialized per the message's response schema.
        o if the error flag is true,
          the error, serialized per the message's error union schema.
    """
    # response metadata
    response_metadata = META_READER.read(decoder)

    # remote response schema
    remote_message_schema = self.remote_protocol.messages.get(message_name)
    if remote_message_schema is None:
      raise schema.AvroException('Unknown remote message: %s' % message_name)

    # local response schema
    local_message_schema = self.local_protocol.messages.get(message_name)
    if local_message_schema is None:
      raise schema.AvroException('Unknown local message: %s' % message_name)

    # error flag
    if not decoder.read_boolean():
      writer_schema = remote_message_schema.response
      reader_schema = local_message_schema.response
      return self.read_response(writer_schema, reader_schema, decoder)
    else:
      writer_schema = remote_message_schema.errors
      reader_schema = local_message_schema.errors
      raise self.read_error(writer_schema, reader_schema, decoder)

  def read_response(self, writer_schema, reader_schema, decoder):
    datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
    result = datum_reader.read(decoder)
    return result

  def read_error(self, writer_schema, reader_schema, decoder):
    datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
    return AvroRemoteException(datum_reader.read(decoder))


class Requestor(BaseRequestor):

  def issue_request(self, call_request, message_name, request_datum):
    call_response = self.transceiver.Transceive(call_request)

    # process the handshake and call response
    buffer_decoder = avro_io.BinaryDecoder(io.BytesIO(call_response))
    call_response_exists = self.read_handshake_response(buffer_decoder)
    if call_response_exists:
      return self.read_call_response(message_name, buffer_decoder)
    else:
      return self.request(message_name, request_datum)


class Responder(object):
  """Base class for the server side of a protocol interaction."""

  def __init__(self, local_protocol):
    self._local_protocol = local_protocol
    self._local_hash = self.local_protocol.md5
    self._protocol_cache = {}
    self.set_protocol_cache(self.local_hash, self.local_protocol)

  # read-only properties
  local_protocol = property(lambda self: self._local_protocol)
  local_hash = property(lambda self: self._local_hash)
  protocol_cache = property(lambda self: self._protocol_cache)

  # utility functions to manipulate protocol cache
  def get_protocol_cache(self, hash):
    return self.protocol_cache.get(hash)
  def set_protocol_cache(self, hash, protocol):
    self.protocol_cache[hash] = protocol

  def respond(self, call_request):
    """
    Called by a server to deserialize a request, compute and serialize
    a response or error. Compare to 'handle()' in Thrift.
    """
    buffer_reader = io.StringIO(call_request)
    buffer_decoder = avro_io.BinaryDecoder(buffer_reader)
    buffer_writer = io.StringIO()
    buffer_encoder = avro_io.BinaryEncoder(buffer_writer)
    error = None
    response_metadata = {}

    try:
      remote_protocol = self.process_handshake(buffer_decoder, buffer_encoder)
      # handshake failure
      if remote_protocol is None:
        return buffer_writer.getvalue()

      # read request using remote protocol
      request_metadata = META_READER.read(buffer_decoder)
      remote_message_name = buffer_decoder.read_utf8()

      # get remote and local request schemas so we can do
      # schema resolution (one fine day)
      remote_message = remote_protocol.messages.get(remote_message_name)
      if remote_message is None:
        fail_msg = 'Unknown remote message: %s' % remote_message_name
        raise schema.AvroException(fail_msg)
      local_message = self.local_protocol.messages.get(remote_message_name)
      if local_message is None:
        fail_msg = 'Unknown local message: %s' % remote_message_name
        raise schema.AvroException(fail_msg)
      writer_schema = remote_message.request
      reader_schema = local_message.request
      request = self.read_request(writer_schema, reader_schema,
                                  buffer_decoder)

      # perform server logic
      try:
        response = self.invoke(local_message, request)
      except AvroRemoteException as e:
        error = e
      except Exception as e:
        error = AvroRemoteException(str(e))

      # write response using local protocol
      META_WRITER.write(response_metadata, buffer_encoder)
      buffer_encoder.write_boolean(error is not None)
      if error is None:
        writer_schema = local_message.response
        self.write_response(writer_schema, response, buffer_encoder)
      else:
        writer_schema = local_message.errors
        self.write_error(writer_schema, error, buffer_encoder)
    except schema.AvroException as e:
      error = AvroRemoteException(str(e))
      buffer_encoder = avro_io.BinaryEncoder(io.StringIO())
      META_WRITER.write(response_metadata, buffer_encoder)
      buffer_encoder.write_boolean(True)
      self.write_error(SYSTEM_ERROR_SCHEMA, error, buffer_encoder)
    return buffer_writer.getvalue()

  def process_handshake(self, decoder, encoder):
    handshake_request = HANDSHAKE_RESPONDER_READER.read(decoder)
    handshake_response = {}

    # determine the remote protocol
    client_hash = handshake_request.get('clientHash')
    client_protocol = handshake_request.get('clientProtocol')
    remote_protocol = self.get_protocol_cache(client_hash)
    if remote_protocol is None and client_protocol is not None:
      remote_protocol = protocol.Parse(client_protocol)
      self.set_protocol_cache(client_hash, remote_protocol)

    # evaluate remote's guess of the local protocol
    server_hash = handshake_request.get('serverHash')
    if self.local_hash == server_hash:
      if remote_protocol is None:
        handshake_response['match'] = 'NONE'
      else:
        handshake_response['match'] = 'BOTH'
    else:
      if remote_protocol is None:
        handshake_response['match'] = 'NONE'
      else:
        handshake_response['match'] = 'CLIENT'

    if handshake_response['match'] != 'BOTH':
      handshake_response['serverProtocol'] = str(self.local_protocol)
      handshake_response['serverHash'] = self.local_hash

    HANDSHAKE_RESPONDER_WRITER.write(handshake_response, encoder)
    return remote_protocol

  def invoke(self, local_message, request):
    """
    Aactual work done by server: cf. handler in thrift.
    """
    pass

  def read_request(self, writer_schema, reader_schema, decoder):
    datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
    return datum_reader.read(decoder)

  def write_response(self, writer_schema, response_datum, encoder):
    datum_writer = avro_io.DatumWriter(writer_schema)
    datum_writer.write(response_datum, encoder)

  def write_error(self, writer_schema, error_exception, encoder):
    datum_writer = avro_io.DatumWriter(writer_schema)
    datum_writer.write(str(error_exception), encoder)


# ------------------------------------------------------------------------------
# Framed message


class FramedReader(object):
  """Wrapper around a file-like object to read framed data."""

  def __init__(self, reader):
    self._reader = reader

  def Read(self):
    """Reads one message from the configured reader.

    Returns:
      The message, as bytes.
    """
    message = io.BytesIO()
    message_size = self._ReadInt32()
    while message_size > 0:
      while message_size > 0:
        data_bytes = self._reader.read(message_size)
        if len(data_bytes) == 0:
          raise ConnectionClosedException('Reader read 0 bytes.')
        message.write(data_bytes)
        message_size -= len(data_bytes)
      message_size = self._ReadInt32()

    return message.getvalue()

  def _ReadInt32(self):
    encoded = self._reader.read(UINT32_BE.size)
    if len(encoded) != UINT32_BE.size:
      raise ConnectionClosedException('Invalid header: %r' % encoded)
    return UINT32_BE.unpack(encoded)[0]


class FramedWriter(object):
  """Wrapper around a file-like object to write framed data."""

  def __init__(self, writer):
    self._writer = writer

  def Write(self, message):
    """Writes a message.

    Args:
      message: Message to write, as bytes.
    """
    while len(message) > 0:
      chunk_size = max(BUFFER_SIZE, len(message))
      chunk = message[:chunk_size]
      self._WriteBuffer(chunk)
      message = message[chunk_size:]

    # A message is always terminated by a zero-length buffer.
    self._WriteUnsignedInt32(0)

  def _WriteBuffer(self, chunk):
    self._WriteUnsignedInt32(len(chunk))
    self._writer.write(chunk)

  def _WriteUnsignedInt32(self, uint32):
    self._writer.write(UINT32_BE.pack(uint32))


# ------------------------------------------------------------------------------
# Transceiver (send/receive channel)


class Transceiver(object, metaclass=abc.ABCMeta):
  @abc.abstractproperty
  def remote_name(self):
    pass

  @abc.abstractmethod
  def ReadMessage(self):
    """Reads a single message from the channel.

    Blocks until a message can be read.

    Returns:
      The message read from the channel.
    """
    pass

  @abc.abstractmethod
  def WriteMessage(self, message):
    """Writes a message into the channel.

    Blocks until the message has been written.

    Args:
      message: Message to write.
    """
    pass

  def Transceive(self, request):
    """Processes a single request-reply interaction.

    Synchronous request-reply interaction.

    Args:
      request: Request message.
    Returns:
      The reply message.
    """
    self.WriteMessage(request)
    result = self.ReadMessage()
    return result

  def Close(self):
    """Closes this transceiver."""
    pass


class HTTPTransceiver(Transceiver):
  """HTTP-based transceiver implementation."""

  def __init__(self, host, port, req_resource='/'):
    """Initializes a new HTTP transceiver.

    Args:
      host: Name or IP address of the remote host to interact with.
      port: Port the remote server is listening on.
      req_resource: Optional HTTP resource path to use, '/' by default.
    """
    self._req_resource = req_resource
    self._conn = http.client.HTTPConnection(host, port)
    self._conn.connect()

  @property
  def remote_name(self):
    return self._conn.sock.getsockname()

  def ReadMessage(self):
    response = self._conn.getresponse()
    response_reader = FramedReader(response)
    framed_message = response_reader.Read()
    response.read()    # ensure we're ready for subsequent requests
    return framed_message

  def WriteMessage(self, message):
    req_method = 'POST'
    req_headers = {'Content-Type': 'avro/binary'}

    bio = io.BytesIO()
    req_body_buffer = FramedWriter(bio)
    req_body_buffer.Write(message)
    req_body = bio.getvalue()

    self._conn.request(req_method, self.req_resource, req_body, req_headers)

  def Close(self):
    self._conn.close()
    self._conn = None


# ------------------------------------------------------------------------------
# Server Implementations


