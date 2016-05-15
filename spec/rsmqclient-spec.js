"use strict"
const expect = require('chai').expect;
const mockery = require('mockery');
const sinon = require('sinon');

describe('CommonMQ RSMQ Client', function() {
    let rsmqConstructorStub,
        CommonMQ;

    before(function() {
        mockery.enable({
            warnOnReplace: false,
            warnOnUnregistered: false,
            useCleanCache: true
        });

        rsmqConstructorStub = sinon.stub();
        mockery.registerMock('rsmq', rsmqConstructorStub);

        CommonMQ = require('../index');

    });

    after(function() {
        mockery.disable();
    });

    it('Should error if different RSMQ MQ and BMQ hosts are provided', function() {
        expect(() => CommonMQ({queue: 'rsmq://bananaqueue', badMessageQueue: 'rsmq://myserver/rottenbananaqueue'}))
            .to.throw(Error, 'primary and bad message queues must have matching hosts and ports');
    });

    it('Should select RSMQ configured to localhost by default', function() {
        let commonmq = CommonMQ();

        expect(rsmqConstructorStub.calledWith({
            host: 'localhost',
            port: 6379
        })).to.be.true;

        expect(commonmq.queueId).to.equal('commonmq');
    });

    it('Should use the provided RSMQ name if provided', function() {
        let commonmq = CommonMQ({
            queue: 'rsmq://bananaqueue'
        });

        expect(commonmq.queueId).to.equal('bananaqueue');
    });

    it('Should use the provided RSMQ host if provided', function() {
        let commonmq = CommonMQ({
            queue: 'rsmq://qserver/bananaqueue'
        });

        expect(rsmqConstructorStub.calledWith({
            host: 'qserver',
            port: 6379
        })).to.be.true;

        expect(commonmq.queueId).to.equal('bananaqueue');
    });

    it('Should use the provided RSMQ port if provided', function() {
        let commonmq = CommonMQ({
            queue: 'rsmq://qserver:1234/bananaqueue'
        });

        expect(rsmqConstructorStub.calledWith({
            host: 'qserver',
            port: 1234
        }));

        expect(commonmq.queueId).to.equal('bananaqueue');
    });

    it('Should send an RSMQ message and return an ID', function(done) {
        let rsmqSendMessageStub = sinon.stub();

        rsmqConstructorStub.returns({
            sendMessage: rsmqSendMessageStub
        });

        let commonmq = CommonMQ();

        let message = {
            'host': 'myhost',
            'request': 'do_the_thing'
        }

        let messageId = 'abcdef-01234-56789-0';

        rsmqSendMessageStub.callsArgWith(1, null, messageId);

        commonmq.send(message, function(err, identifier) {
            expect(err).to.not.be.okay;
            expect(identifier).to.equal(messageId);

            expect(rsmqSendMessageStub.calledWith({
                qname: commonmq.queueId,
                message: message
            })).to.be.true;

            done();
        });
    });

    it('Should receive an RSMQ message', function(done) {
        let rsmqReceiveMessageStub = sinon.stub();

        rsmqConstructorStub.returns({
            receiveMessage: rsmqReceiveMessageStub
        });

        let commonmq = CommonMQ();

        let messageId = 'abcdef-01234-56789-0';
        let message = {
            'host': 'myhost',
            'request': 'do_the_thing'
        };

        rsmqReceiveMessageStub.callsArgWith(1, null, {
            id: messageId,
            message: message,
            rc: 0
        });

        commonmq.receive(function(err, resp) {
            expect(err).to.not.be.okay;

            expect(resp.id).to.equal(messageId);
            expect(resp.data).to.equal(message);

            expect(rsmqReceiveMessageStub.calledWith({
                qname: commonmq.queueId
            })).to.be.true;

            done();
        });
    });

    it('Should wait for an RSMQ message if there is not one', function(done) {
        let rsmqReceiveMessageStub = sinon.stub();

        rsmqConstructorStub.returns({
            receiveMessage: rsmqReceiveMessageStub
        });

        let commonmq = CommonMQ({
            pollInterval: 1
        });

        let message = {
            'host': 'myhost',
            'request': 'do_the_thing'
        };

        rsmqReceiveMessageStub.onFirstCall().callsArgWith(1, null, null);
        rsmqReceiveMessageStub.onSecondCall().callsArgWith(1, null, null);
        rsmqReceiveMessageStub.onSecondCall().callsArgWith(1, null, {
            id: 'someId',
            message: message,
            rc: 0
        });

        commonmq.receive(function(err, resp) {
            expect(err).to.not.be.okay;

            expect(resp.data).to.equal(message);

            expect(rsmqReceiveMessageStub.calledWith({
                qname: commonmq.queueId
            })).to.be.true;

            done();
        });
    });

    it('Should extend the visibility timeout of a message if requested', function(done) {
        let rsmqChangeMessageVisibilityStub = sinon.stub();

        rsmqConstructorStub.returns({
            changeMessageVisibility: rsmqChangeMessageVisibilityStub
        });

        let commonmq = CommonMQ();

        let messageId = '0123456789-afbecd';

        rsmqChangeMessageVisibilityStub.callsArgWith(1, null);

        commonmq.extendVisibilityTimeout(messageId, 300, function(err) {
            expect(err).to.not.be.okay;

            expect(rsmqChangeMessageVisibilityStub.calledWith({
                qname: commonmq.queueId,
                id: messageId,
                vt: 300
            })).to.be.true;

            done();
        });
    });

    it('Should delete a message', function(done) {
        let rsmqDeleteMessageStub = sinon.stub();

        rsmqConstructorStub.returns({
            deleteMessage: rsmqDeleteMessageStub
        });

        let commonmq = CommonMQ();

        let messageId = '0123456789-afbecd';

        rsmqDeleteMessageStub.callsArgWith(1, 1);

        commonmq.delete(messageId, function(err) {
            expect(err).to.not.be.okay;

            expect(rsmqDeleteMessageStub.calledWith({
                qname: commonmq.queueId,
                id: messageId
            })).to.be.true;

            done();
        });
    });

    it('Should move a message to the bad message queue', function(done) {
        let rsmqDeleteMessageStub = sinon.stub();
        let rsmqSendMessageStub = sinon.stub();

        rsmqConstructorStub.returns({
            deleteMessage: rsmqDeleteMessageStub,
            sendMessage: rsmqSendMessageStub
        });

        let commonmq = CommonMQ({
            badMessageQueue: 'rsmq://rottenbananaqueue'
        });

        let messageId = '0123456789-afbecd';
        let badMessageId = '1123456789-afbecd';
        let message = '<xml attr="notjson"></xml>';

        rsmqSendMessageStub.callsArgWith(1, null, badMessageId);
        rsmqDeleteMessageStub.callsArgWith(1, 1);

        commonmq.badMessage(messageId, message, function(err, newMessageId) {
            expect(err).to.not.be.okay;

            expect(newMessageId).to.equal(badMessageId);

            expect(rsmqDeleteMessageStub.calledWith({
                qname: commonmq.queueId,
                id: messageId
            })).to.be.true;

            expect(rsmqSendMessageStub.calledWith({
                qname: 'rottenbananaqueue',
                message: message
            })).to.be.true;

            done();
        });

    });

});
