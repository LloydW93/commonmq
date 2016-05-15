"use strict"

const RSMQClient = (function() {
    const RSMQ = require('rsmq');
    const extend = require('extend');
    const CommonMQError = require('./commonmq-error');

    function parseQueueUrl(url) {
        // rsmq://[server[:port]/]queueName
        let queueName;
        let host = 'localhost';
        let port = 6379;
        let parts = url.split('://')[1].split('/');

        if (parts.length === 2) {
            let redisAddress = parts[0].split(':');
            if (redisAddress.length === 2) {
                port = parseInt(redisAddress[1], 10);
            }
            host = redisAddress[0];
        }

        queueName = parts[parts.length - 1];

        return {
            host: host,
            port: port,
            queueName: queueName
        };
    }

    function createRsmqHandle(connection) {
        return new RSMQ({
            host: connection.host,
            port: connection.port
        });
    }

    const RSMQClient = function(opts) {
        let options = extend({
            pollInterval: 1000
        }, opts);

        let connectionData = parseQueueUrl(options.queue);
        let badMessageConnectionData;
        this._rsmq = createRsmqHandle(connectionData);

        if (options.badMessageQueue) {
            badMessageConnectionData = parseQueueUrl(options.badMessageQueue);
            if (badMessageConnectionData.host !== connectionData.host || badMessageConnectionData.port !== connectionData.port) {
                throw new CommonMQError('primary and bad message queues must have matching hosts and ports');
            }
        }

        Object.defineProperties( this, {
            queueName: {
                get: () => connectionData.queueName
            },
            badMessageQueueName: {
                get: () => badMessageConnectionData.queueName
            },
            options: {
                get: () => options
            }
        });
    };

    RSMQClient.prototype = {
        send: function(message, callback) {
            this._rsmq.sendMessage({qname: this.queueName, message: message}, callback);
        },
        receive: function(callback) {
            let client = this;
            client._rsmq.receiveMessage({qname: client.queueName}, function(err, msg) {
                if (err || msg) {
                    callback(err, msg ? {
                        id: msg.id,
                        data: msg.message
                    } : null);
                } else {
                    setTimeout(function() {
                        client.receive(callback);
                    }, client.options.pollInterval);
                }
            });
        },
        extendVisibilityTimeout: function(messageId, length, callback) {
            this._rsmq.changeMessageVisibility({
                qname: this.queueName,
                id: messageId,
                vt: length
            }, function(err, data) {
                if (!err && data === 0) {
                    err = 'message not found';
                }
                callback(err);
            });
        },
        delete: function(messageId, callback) {
            this._rsmq.deleteMessage({
                qname: this.queueName,
                id: messageId
            }, function(err, data) {
                if (!err && !data) {
                    err = 'message not found';
                }
                callback(err);
            });
        },
        badMessage: function(messageId, message, callback) {
            let client = this;
            client._rsmq.sendMessage({qname: client.badMessageQueueName, message: message}, function(err, data) {
                if (!err) {
                    client.delete(messageId, function(err2) {
                        if (!err && err2) {
                            err = err2;
                        }
                        callback(err, data);
                    });
                }
            });
        }
    };

    return RSMQClient;
})();

module.exports = RSMQClient;
