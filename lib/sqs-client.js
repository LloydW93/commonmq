"use strict"

const SQSClient = (function() {
    const AWS = require('aws-sdk');
    const extend = require('extend');
    const async = require('async');
    const CommonMQError = require('./commonmq-error');

    function parseQueueUrl(url) {
        // sqs://region/accountId/queueName
        let parts = url.split('://')[1].split('/');

        return {
            region: parts[0],
            accountId: parts[1],
            queueName: parts[2],
            queueUrl: 'https://sqs.' + parts[0] + '.amazonaws.com/' + parts[1] + '/' + parts[2]
        };
    }

    function createSQSHandle(connection) {
        return new AWS.SQS({
            host: connection.host,
            port: connection.port
        });
    }

    const SQSClient = function(opts) {
        let options = extend({
            pollInterval: 1000
        }, opts);

        let connectionData = parseQueueUrl(options.queue);
        let badMessageConnectionData;
        this._sqs = createSQSHandle(connectionData);

        this._receiptHandleStore = {};

        if (options.badMessageQueue) {
            badMessageConnectionData = parseQueueUrl(options.badMessageQueue);
            if (badMessageConnectionData.region !== connectionData.region) {
                throw new CommonMQError('primary and bad message queues must have matching regions');
            }
        }

        Object.defineProperties(this, {
            queueId: {
                get: () => connectionData.queueUrl
            },
            badMessageQueueId: {
                get: () => badMessageConnectionData.queueUrl
            },
            options: {
                get: () => options
            }
        });
    };

    SQSClient.prototype = {
        send: function(message, callback) {
            this._sqs.sendMessage({
                QueueUrl: this.queueId,
                MessageBody: message
            }, function(err, data) {
                callback(err, data ? data.MessageId : null);
            });
        },
        receive: function(callback) {
            let client = this;
            client._sqs.receiveMessage({
                QueueUrl: client.queueId
            }, function(err, msg) {
                if (msg) {
                    client._receiptHandleStore[msg.Messages[0].MessageId] = msg.Messages[0].ReceiptHandle;
                }
                callback(err, msg ? {
                    id: msg.Messages[0].MessageId,
                    data: msg.Messages[0].Body
                } : null);
            });
        },
        extendVisibilityTimeout: function(messageId, length, callback) {
            let receiptHandle = this._receiptHandleStore[messageId];
            if (!receiptHandle) {
                callback('no receipt handle for message available');
            } else {
                this._sqs.changeMessageVisibility({
                    QueueUrl: this.queueId,
                    ReceiptHandle: receiptHandle,
                    VisibilityTimeout: length
                }, function(err) {
                    callback(err);
                });
            }
        },
        delete: function(messageId, callback) {
            let receiptHandle = this._receiptHandleStore[messageId];
            if (!receiptHandle) {
                callback('no receipt handle for message available');
            } else {
                this._sqs.deleteMessage({
                    QueueUrl: this.queueId,
                    ReceiptHandle: receiptHandle
                }, function(err) {
                    callback(err);
                });
            }
        },
        badMessage: function(messageId, message, callback) {
            let client = this;

            async.series([
                cb => client._sqs.sendMessage({
                    QueueUrl: client.badMessageQueueId,
                    MessageBody: message
                }, cb),
                cb => client.delete(messageId, cb)
            ], function(err, data) {
                callback(err, data[0].MessageId);
            });
        }
    };

    return SQSClient;
})();

module.exports = SQSClient;
