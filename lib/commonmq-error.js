"use strict"

const util = require('util');

function CommonMQError(message) {
  Error.captureStackTrace(this, this.constructor);
  this.name = this.constructor.name;
  this.message = (message || '');
}
util.inherits(CommonMQError, Error);

module.exports = CommonMQError;
