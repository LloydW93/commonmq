"use strict"

const CommonMQ = (function() {
    const queueProtocolMapper = {
        rsmq: require('./lib/rsmq-client'),
        sqs: require('sqs-consumer')
    };
    const extend = require('extend');
    const util = require('util');
    const CommonMQError = require('./lib/commonmq-error');

    const CommonMQ = function(opts) {
        let options = extend({
            /**
             * Format:
             * - rsmq://[server[:port]/]queueName
             * - sqs://region/accountId/queueName
             */
            queue: 'rsmq://commonmq',
            badMessageQueue: null
        }, opts);

        let queueProtocolSplit = (options.queue + '').split('://');
        if ((options.queue + '').indexOf('://') === -1) {
            throw new CommonMQError('options.queue does not contain a protocol.');
        }
        let protocol = queueProtocolSplit[0];
        if (!queueProtocolMapper.hasOwnProperty(protocol)) {
            throw new CommonMQError('queue protocol ' + protocol + ' is not supported');
        }

        if (options.badMessageQueue) {
            let badMessageProtocol = options.badMessageQueue.split('://')[0];
            if (badMessageProtocol !== protocol) {
                throw new CommonMQError('bad message queue and primary queue must have same protocol');
            }
        }

        return new queueProtocolMapper[protocol](options);
    };

    return CommonMQ;
})();

module.exports = CommonMQ;
