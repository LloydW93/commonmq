"use strict"
const expect = require('chai').expect;
const mockery = require('mockery');
const sinon = require('sinon');
const async = require('async');

const commonmqConfig = {
    queue: 'sqs://eu-west-1/012345678901/bananaqueue',
    badMessageQueue: 'sqs://eu-west-1/012345678901/rottenbananaqueue'
};

describe('CommonMQ SQS Client', function() {
    let sqsConstructorStub,
        CommonMQ;

    before(function() {
        mockery.enable({
            warnOnReplace: false,
            warnOnUnregistered: false,
            useCleanCache: true
        });

        sqsConstructorStub = sinon.stub();
        let awsStub = {
            SQS: sqsConstructorStub
        };
        mockery.registerMock('aws-sdk', awsStub);

        CommonMQ = require('../index');

    });

    after(function() {
        mockery.disable();
    });

    it('Should error if different SQS MQ and BMQ regions are provided', function() {
        expect(() => CommonMQ({queue: commonmqConfig.queue, badMessageQueue: 'sqs://eu-central-1/012345678901/rottenbananaqueue'}))
            .to.throw(Error, 'primary and bad message queues must have matching regions');
    });

    it('Should use the provided SQS name, account and region to build a queue URL', function() {
        let commonmq = CommonMQ({
            queue: 'sqs://eu-west-1/012345678901/bananaqueue'
        });

        expect(commonmq.queueId).to.equal('https://sqs.eu-west-1.amazonaws.com/012345678901/bananaqueue');
    });

    it('Should send an SQS message and return an ID', function(done) {
        let sqsSendMessageStub = sinon.stub();

        sqsConstructorStub.returns({
            sendMessage: sqsSendMessageStub
        });

        let commonmq = CommonMQ(commonmqConfig);

        let message = {
            'host': 'myhost',
            'request': 'do_the_thing'
        }

        let messageId = 'abcdef-01234-56789-0';

        sqsSendMessageStub.callsArgWith(1, null, {MessageId: messageId});

        commonmq.send(message, function(err, identifier) {
            expect(err).to.not.be.okay;
            expect(identifier).to.equal(messageId);

            expect(sqsSendMessageStub.calledWith({
                MessageBody: message,
                QueueUrl: commonmq.queueId
            })).to.be.true;

            done();
        });
    });

    it('Should receive an SQS message', function(done) {
        let sqsReceiveMessageStub = sinon.stub();

        sqsConstructorStub.returns({
            receiveMessage: sqsReceiveMessageStub
        });

        let commonmq = CommonMQ(commonmqConfig);

        let messageId = 'abcdef-01234-56789-0';
        let message = {
            'host': 'myhost',
            'request': 'do_the_thing'
        };

        sqsReceiveMessageStub.callsArgWith(1, null, {
            Messages: [{
                MessageId: messageId,
                Body: message
            }]
        });

        commonmq.receive(function(err, resp) {
            expect(err).to.not.be.okay;

            expect(resp.id).to.equal(messageId);
            expect(resp.data).to.equal(message);

            expect(sqsReceiveMessageStub.calledWith({
                QueueUrl: commonmq.queueId
            })).to.be.true;

            done();
        });
    });

    it('Should extend the visibility timeout of a message if requested', function(done) {
        let sqsReceiveMessageStub = sinon.stub();
        let sqsChangeMessageVisibilityStub = sinon.stub();

        sqsConstructorStub.returns({
            receiveMessage: sqsReceiveMessageStub,
            changeMessageVisibility: sqsChangeMessageVisibilityStub
        });

        let commonmq = CommonMQ(commonmqConfig);

        let messageId = '0123456789-afbecd';
        let message = {
            'host': 'myhost',
            'request': 'do_the_thing'
        };

        let receiptHandle = 'xxx-yyy-qqvv0';

        sqsReceiveMessageStub.callsArgWith(1, null, {
            Messages: [{
                MessageId: messageId,
                ReceiptHandle: receiptHandle,
                Body: message
            }]
        });
        sqsChangeMessageVisibilityStub.callsArgWith(1, null);

        async.series([
            cb => commonmq.receive(cb),
            cb => commonmq.extendVisibilityTimeout(messageId, 300, cb)
        ], function(err) {
            expect(err).to.not.be.okay;

            expect(sqsChangeMessageVisibilityStub.calledWith({
                QueueUrl: commonmq.queueId,
                ReceiptHandle: receiptHandle,
                VisibilityTimeout: 300
            })).to.be.true;

            done();
        });
    });

    it('Should delete a message', function(done) {
        let sqsReceiveMessageStub = sinon.stub();
        let sqsDeleteMessageStub = sinon.stub();

        sqsConstructorStub.returns({
            receiveMessage: sqsReceiveMessageStub,
            deleteMessage: sqsDeleteMessageStub
        });

        let commonmq = CommonMQ(commonmqConfig);

        let messageId = '0123456789-afbecd';
        let message = {
            'host': 'myhost',
            'request': 'do_the_thing'
        };

        let receiptHandle = 'xxx-yyy-qqvv0';

        sqsReceiveMessageStub.callsArgWith(1, null, {
            Messages: [{
                MessageId: messageId,
                ReceiptHandle: receiptHandle,
                Body: message
            }]
        });
        sqsDeleteMessageStub.callsArgWith(1, 1);

        async.series([
            cb => commonmq.receive(cb),
            cb => commonmq.delete(messageId, cb)
        ], function(err) {
            expect(err).to.not.be.okay;

            expect(sqsDeleteMessageStub.calledWith({
                QueueUrl: commonmq.queueId,
                ReceiptHandle: receiptHandle
            })).to.be.true;

            done();
        });
    });

    it('Should move a message to the bad message queue', function(done) {
        let sqsReceiveMessageStub = sinon.stub();
        let sqsSendMessageStub = sinon.stub();
        let sqsDeleteMessageStub = sinon.stub();

        sqsConstructorStub.returns({
            receiveMessage: sqsReceiveMessageStub,
            deleteMessage: sqsDeleteMessageStub,
            sendMessage: sqsSendMessageStub
        });

        let commonmq = CommonMQ(commonmqConfig);

        let messageId = '0123456789-afbecd';
        let badMessageId = '1123456789-afbecd';
        let message = '<xml attr="notjson"></xml>';
        let receiptHandle = 'xxx-yyy-qqvv0';

        sqsReceiveMessageStub.callsArgWith(1, null, {
            Messages: [{
                MessageId: messageId,
                ReceiptHandle: receiptHandle,
                Body: message
            }]
        });
        sqsSendMessageStub.callsArgWith(1, null, {MessageId: badMessageId});
        sqsDeleteMessageStub.callsArgWith(1, 1);

        async.series([
            cb => commonmq.receive(cb),
            cb => commonmq.badMessage(messageId, message, cb)
        ], function(err, results) {
            expect(err).to.not.be.okay;

            expect(results[1]).to.equal(badMessageId);

            expect(sqsSendMessageStub.calledWith({
                QueueUrl: commonmq.badMessageQueueId,
                MessageBody: message
            })).to.be.true;

            expect(sqsDeleteMessageStub.calledWith({
                QueueUrl: commonmq.queueId,
                ReceiptHandle: receiptHandle
            })).to.be.true;

            done();
        });

        commonmq.badMessage(messageId, message, function(err, newMessageId) {
            
        });

    });

});
