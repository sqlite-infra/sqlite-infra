'use strict';

// One bundle over the cloud interface pieces: the shape validators
// (adapters/util.js, which also documents the shapes and the contract) and the
// concrete implementations beside this file (lib/memory.js, lib/http-sink.js),
// so the tests and the adapters reach them through one object. At runtime only
// lib/sync.js requires it.

const { assertObjectStore, assertMessageSink, assertMessageSource } = require('../adapters/util');
const { MemoryObjectStore, MemoryQueue, collect, notFound } = require('./memory');
const { HttpSink, outcomeForStatus } = require('./http-sink');

module.exports = {
  assertObjectStore,
  assertMessageSink,
  assertMessageSource,
  collect,
  notFound,
  MemoryObjectStore,
  MemoryQueue,
  HttpSink,
  outcomeForStatus
};
