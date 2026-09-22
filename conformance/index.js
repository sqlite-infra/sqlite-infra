'use strict';

// The upstream tests do require('../.') to reach the package under test.
// From conformance/test/ that resolves here, and here resolves to this package.
module.exports = require('..');
