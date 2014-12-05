// currently not using any sort of encryption, but should
// look at the possibility of using tls at some point in the future
// for cross-machine communication

"use strict"

var HashRing = require("./HashRing"),
    net = require('net'),
    uuid = require('node-uuid'),
    _ = require("lodash"),
    events = require('events');

////////////////////////////////////////////////////////////////////////////
// NodeClient

var NodeClient = function (options, next) {

    // attempt to connect to the main server to register ourselves
    var client = new net.Socket(),
        eventsEmitter = new events.EventEmitter(),
        messageParser = new MessageParser(),
        messageTokenizer = new MessageTokenizer();

    var disconnect = function () {
            client.end();
        },

        on = function (event, callback) {

            eventsEmitter.on(event, callback);

            return this;
        },

        processMessages = function () {

            // process the message(s)
            while (messageParser.hasMessage()) {

                var msg = messageParser.nextMessage();

                msg.reply = function (reply) {

                    // send the reply back to the requester
                    var payload = messageTokenizer.serialize({
                        id: msg.id,
                        reply: reply
                    });

                    client.write(payload);
                }

                // tell the world
                eventsEmitter.emit(msg.name, msg);
            }
        }

    client
        .connect(options.port, options.host, function() {
            next(null);
        })

        .on('data', function(data) {

            // add the data to our buffer
            messageParser.addData(data);

            // process any complete messages
            processMessages();
        })

        .on('close', function() {
        })

        .on('error', function(err) {
            next(err);
        });

    return {
        disconnect: disconnect,
        on: on
    }
}

////////////////////////////////////////////////////////////////////////////
// NodeMaster

var NodeMaster = function (options, next) {

    var eventsEmitter = new events.EventEmitter(),
        nodeSelector = new NodeSelector(),
        tokenizer = new MessageTokenizer(),
        sockets = {},
        pendingRequests = {};

    var

        socketConnected = function (socket) {

            // store an ID so we can identify it later
            socket.uuid = uuid.v1();

            socket
                .on('data', function(data) {

                    var socketInfo = sockets[socket.uuid];

                    // add the data to the parser
                    socketInfo.parser.addData(data);

                    processMessages();
                })

                .on('close', function() {

                    // tell the world
                    eventsEmitter.emit('disconnect', sockets[socket.uuid]);

                    // add the node to our node selector
                    nodeSelector.removeNode(socket.uuid);

                    delete sockets[socket.uuid];
                })

                .on('error', function(err) {
                });

            // store the socket
            var socketInfo = {
                socket: socket,
                parser: new MessageParser(),
                connected: new Date(),
                uuid: socket.uuid
            };

            sockets[socket.uuid] = socketInfo;

            // add the node to our node selector
            nodeSelector.addNode(socket.uuid, socketInfo);

            // tell the world
            eventsEmitter.emit('connect', socketInfo);

        },

        status = function () {
            return {
                nodeCount: _.size(sockets)
            }
        },

        stop= function() {

            // disconnect any connected clients
            _.forEach(sockets, function (socket) {
                socket.socket.end();
            })

            server.close();
        },

        on = function (event, callback) {

            eventsEmitter.on(event, callback);

            return this;
        },

        send = function (shardId, message, content) {

            if (_.size(sockets) == 0)
                throw "There are no nodes currently connected.";

            var messageId = uuid.v1();

            // get the payload
            var payload = tokenizer.serialize({
                id: messageId,
                name: message,
                content: content
            });

            // do we need to choose a particular client to send this to?
            var toSendNodes = nodeSelector.getKeyNodes(shardId);

            // send the data to each node
            _.forEach(toSendNodes, function (node) {
                node.socket.write(payload);
            })

            // return the message ID
            return messageId;
        },

        request = function (shardId, message, content, callback) {

            // send the message
            var messageId = send(shardId, message, content);

            // do we need to choose a particular client to send this to?
            var toSendNodes = nodeSelector.getKeyNodes(shardId);

            var pendingRequest = {
                sent: new Date(),
                replies: {},
                callback: callback
            };

            _.forEach(toSendNodes, function (node) {
                pendingRequest.replies[node.uuid] = {
                    received: null // not yet received
                };
            });

            pendingRequests[messageId] = pendingRequest;
        },

        processMessages = function () {

            // process the message(s)
            _.forEach(sockets, function (socketInfo) {

                while (socketInfo.parser.hasMessage()) {

                    var msg = socketInfo.parser.nextMessage();

                    // make sure the response has a valid request.. otherwise ignore it
                    if (!_.has(pendingRequests, msg.id))
                        continue;

                    // get the pending request
                    var pendingRequest = pendingRequests[msg.id];

                    // make sure we are expecting this response..
                    if (!_.has(pendingRequest.replies, socketInfo.uuid))
                        continue;

                    // update the data
                    pendingRequest.replies[socketInfo.uuid].received = new Date();
                    pendingRequest.replies[socketInfo.uuid].reply = msg.reply;

                    // see if this has been fulfilled
                    var gotAllReplies = true;

                    _.forEach(pendingRequest.replies, function (pendingNode) {
                        gotAllReplies&= pendingNode.received != null;
                    });

                    if (gotAllReplies) {
                        delete pendingRequests[msg.id];

                        pendingRequest.callback(null, {
                            responseTime: new Date() - pendingRequest.sent,
                            replies: pendingRequest.replies
                        });
                    }
                }
            })
        };

    // create the server
    var server = net.createServer(socketConnected);

    server
        .on('error', function (e) {
            next(e);
        })

        server.listen(options.port, function () {
            next(null);
        });

    return {
        status: status,
        stop: stop,
        on: on,
        send: send,
        request: request
    };
}

////////////////////////////////////////////////////////////////////////////
// NodeSelector

var NodeSelector = function () {

    var ring = new HashRing(),
        nodes = {};

    var getNode = function (id) {

            if (!hasNode(id))
                return null;

            return nodes[id];
        },

        hasNode = function (id) {
            return _.has(nodes, id);
        },

        addNode = function (id, node) {

            // add to the hash ring
            ring.addNode(node);

            // store a reference for ourselves
            nodes[id] = node;
        },

        removeNode = function (id) {

            if (!hasNode(id))
                return;

            delete nodes[id];

            // recalculate the HashRing
            createHashRing();
        },

        createHashRing = function () {
            ring = new HashRing();

            _.forOwn(nodes, function (node, id) {
                ring.addNode(node);
            });
        },

        getKeyNodes = function (key) {

            if (!key)
                return _.values(nodes);

            return [ring.getNode(key)];
        }

    return {
        getNode: getNode,
        getKeyNodes: getKeyNodes,
        addNode: addNode,
        hasNode: hasNode,
        removeNode: removeNode
    }
}

////////////////////////////////////////////////////////////////////////////
// MessageTokenizer

var MessageTokenizer = function () {

    var
        serialize = function (msg) {
            return JSON.stringify(msg) + "\0";
        };

    return {
        serialize: serialize
    }
}

var MessageParser = function () {

    var buffer = '';

    var

        addData = function (data) {
            buffer+= data;
        },

        hasMessage = function () {
            return buffer.indexOf("\0") != -1;
        },

        jsonDeserializeHelper = function (key,value) {
            if ( typeof value === 'string' ) {
                var regexp;
                regexp = /^{timestamp}(\d*)$/.exec(value);
                if ( regexp ) {
                    return new Date(+regexp[1]);
                }
            }
            return value;
        },

        nextMessage = function () {

            if (!hasMessage())
                return null;

            // find the message end index
            var endIndex = buffer.indexOf("\0");

            // get the message
            var msg = buffer.substring(0, endIndex);

            // clip the buffer
            buffer = buffer.substring(endIndex+1);

            // parse the josn
            return JSON.parse(msg, jsonDeserializeHelper);
        }

    return {
        addData: addData,
        hasMessage: hasMessage,
        nextMessage: nextMessage
    }
}

////////////////////////////////////////////////////////////////////////////
// override the date object's toJSON function to return a value we can parse later
//
// http://stackoverflow.com/questions/9194372/why-does-json-stringify-screw-up-my-datetime-object

Date.prototype.toJSON = function()
{
    var time = this.getTime();

    return "{timestamp}" + time;
}

////////////////////////////////////////////////////////////////////////////
// Exports

module.exports = (function(){

    var AppSharder = function () {

        return {
            node: function(options, next) {
                return new NodeClient(options, next);
            },

            master: function(options, next) {
                return new NodeMaster(options, next);
            },

            messageTokenizer: function() {
                return new MessageTokenizer();
            },

            messageParser: function() {
                return new MessageParser();
            },

            nodeSelector: function() {
                return new NodeSelector();
            }
        }
    }

    return new AppSharder();
})();