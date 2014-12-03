// currently not using any sort of encryption, but should
// look at the possibility of using tls at some point in the future
// for cross-machine communication

"use strict"

var HashRing = require("./HashRing"),
    net = require('net'),
    uuid = require('node-uuid'),
    _ = require("lodash"),
    events = require('events'),
    sys = require('sys');

////////////////////////////////////////////////////////////////////////////
// NodeClient

var NodeClient = function (options, next) {

    // attempt to connect to the main server to register ourselves
    var client = new net.Socket(),
        eventsEmitter = new events.EventEmitter();

    var disconnect = function () {
            client.end();
        },

        on = function (event, callback) {

            eventsEmitter.on(event, callback);

            return this;
        };

    client
        .connect(options.port, options.host, function() {
            next(null);
        })

        .on('data', function(data) {
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

    var eventsEmitter = new events.EventEmitter();

    var sockets = {},
        server = net.createServer(function (socket) {

            // store an ID so we can identify it later
            socket.uuid = uuid.v1();

            socket
                .on('data', function(data) {
                })

                .on('close', function() {

                    // tell the world
                    eventsEmitter.emit('disconnect', sockets[socket.uuid]);

                    delete sockets[socket.uuid];
                })

                .on('error', function(err) {
                });

            // store the socket
            sockets[socket.uuid] = socket;

            // tell the world
            eventsEmitter.emit('connect', socket);
        });

    var status = function () {
            return {
                nodeCount: _.size(sockets)
            }
        },

        stop= function() {

            // disconnect any connected clients
            _.forEach(sockets, function (socket) {
                socket.end();
            })

            server.close();
        },

        on = function (event, callback) {

            eventsEmitter.on(event, callback);

            return this;
        },

        send = function (shardId) {

            if (_.size(sockets) == 0)
                throw "There are no nodes currently connected.";



        };

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
        send: send
    };
}


////////////////////////////////////////////////////////////////////////////
// MessageTokenizer

var MessageTokenizer = function () {

    var
        serialize = function (msg) {
            return JSON.stringify(msg) + "\0";
        }

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
            return JSON.parse(msg);
        }

    return {
        addData: addData,
        hasMessage: hasMessage,
        nextMessage: nextMessage
    }
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
            }
        }
    }

    return new AppSharder();
})();