"use strict"
const expect = require('chai').expect;
const mockery = require('mockery');
const sinon = require('sinon');
const CommonMQ = require('../index');

describe('CommonMQ', function() {

    it('Should error if no MQ protocol is provided', function() {
        expect(() => CommonMQ({queue: 'bananaqueue'}))
            .to.throw(Error, 'options.queue does not contain a protocol.');
    });

    it('Should error if an unsupported MQ protocol is provided', function() {
        expect(() => CommonMQ({queue: 'supermq://bananaqueue'}))
            .to.throw(Error, 'queue protocol supermq is not supported');
    });

    it('Should error if different MQ and BMQ protocols are provided', function() {
        expect(() => CommonMQ({queue: 'rsmq://bananaqueue', badMessageQueue: 'sqs://00000000000/rottenbananaqueue'}))
            .to.throw(Error, 'bad message queue and primary queue must have same protocol');
    });

});
