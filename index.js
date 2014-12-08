// currently not using any sort of encryption, but should
// look at the possibility of using tls at some point in the future
// for cross-machine communication

"use strict"

var HashRing = require("./HashRing"),
    net = require('net'),
    tls = require('tls'),
    fs = require('fs'),
    uuid = require('node-uuid'),
    _ = require("lodash"),
    events = require('events');

// constants
var DEFAULT_PORT = 5134,
    DEFAULT_MASTER_HOST = "0.0.0.0",
    DEFAULT_NODE_HOST = "127.0.0.1",
    DEFAULT_REQUEST_TIMEOUT = 5000;

////////////////////////////////////////////////////////////////////////////
// NodeClient

var NodeClient = function (options) {

    // attempt to connect to the main server to register ourselves
    var client = null,
        eventsEmitter = new events.EventEmitter(),
        messageParser = new MessageParser(),
        messageTokenizer = new MessageTokenizer(),
        onConnected = null,
        onConnectCalled = false,
        authenticator = new Authenticator(),
        defaultOptions = {
            port: DEFAULT_PORT,
            host: DEFAULT_NODE_HOST,
            name: "",
            auth: {}
        };

    if (!options)
        options= {};

    // merge the options
    options = _.defaults(
        options,
        defaultOptions
    );

    var disconnect = function () {
            if (client)
                client.end();
        },

        connect = function (next) {

            onConnected= next;

            // create the server
            if (options.tls && options.tls.enabled){
                client = tls.connect(
                    options.port,
                    options.host,
                    {
                        rejectUnauthorized: false
                    },
                    function () { });
            }
            else {
                client = new net.Socket();

                client.connect(options.port, options.host, function () { });
            }

            client

                .on('data', function(data) {

                    // add the data to our buffer
                    messageParser.addData(data);

                    // process any complete messages
                    processMessages();
                })

                .on('close', function() {
                    runOnConnect("Connection closed by remote server");
                })

                .on('error', function(err) {
                    runOnConnect(err);
                });

            authenticator.startAuthentication({
                socket: client,
                auth: options.auth,
                name: options.name,
                onAuthenticate: options.onAuthenticate,
                onSuccess: function () {
                    runOnConnect(null);
                },
                onFail: function (err) {
                    runOnConnect(err);

                }
            });

            return this;
        },

        runOnConnect = function (err) {

            if (onConnectCalled)
                return;

            if (onConnected)
                onConnected(err);

            onConnectCalled= true;
        },

        on = function (event, callback) {

            eventsEmitter.on(event, callback);

            return this;
        },

        processMessages = function () {

            // process the message(s)
            while (messageParser.hasMessage()) {

                var msg = messageParser.nextMessage();

                if (msg == null) {
                    // kill the connection?
                    client.end();
                    continue;
                }

                if (!authenticator.isAuthenticated()) {
                    authenticator.processMessage(msg);
                    continue;
                }

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
        };

    // add any events passed in the options
    if (options.messages) {
        _.forEach(options.messages, function (eventInfo) {
            eventsEmitter.on(
                eventInfo.name,
                eventInfo.handler
            );
        })
    }

    return {
        disconnect: disconnect,
        connect: connect,
        on: on
    }
}

////////////////////////////////////////////////////////////////////////////
// NodeMaster

var NodeMaster = function (options) {

    var eventsEmitter = new events.EventEmitter(),
        nodeSelector = new NodeSelector(),
        tokenizer = new MessageTokenizer(),
        sockets = {},
        upSince = null,
        pendingRequests = {},
        defaultOptions = {
            requestTimeout: DEFAULT_REQUEST_TIMEOUT,
            port: DEFAULT_PORT,
            host: DEFAULT_MASTER_HOST
        };

    if (!options)
        options= {};

    // merge the options
    options = _.defaults(
        options,
        defaultOptions
    );

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
                    reportDisconnect(socketInfo, null);
                })

                .on('error', function(err) {
                    reportDisconnect(socketInfo, err);
                });

            // store the socket
            var socketInfo = {
                socket: socket,
                parser: new MessageParser(),
                upSince: new Date(),
                uuid: socket.uuid,
                remoteAddress: socket.remoteAddress,
                name: "",
                authenticator: new Authenticator()
            };

            sockets[socket.uuid] = socketInfo;

            // add the node to our node selector
            nodeSelector.addNode(socket.uuid, socketInfo);

            // tell the world
            eventsEmitter.emit('connect', socketInfo);

            // start the authentication process
            socketInfo.authenticator.startAuthentication({
                socket: socket,
                auth: options.auth,
                name: options.name,
                //onAuthenticate: options.onAuthenticate,

                onAuthenticate: function (authInfo) {

                    socketInfo.name = authInfo.name;

                    if (_.isFunction(options.onAuthenticate))
                        return options.onAuthenticate(authInfo);

                    return true;
                },
                onSuccess: function () {
                    eventsEmitter.emit('authenticated', socketInfo);
                },
                onFail: function () {
                    // do something else?!
                }
            });
        },

        reportDisconnect = function (socketInfo, err) {

            if (!_.has(sockets, socketInfo.uuid))
                return;

            // tell the world
            eventsEmitter.emit('disconnect', socketInfo);

            // add the node to our node selector
            nodeSelector.removeNode(socketInfo.uuid);

            delete sockets[socketInfo.uuid];
        },

        getOptions = function () {
            return options;
        },

        status = function () {

            var upTime = upSince != null ? new Date() - upSince : null,
                nodes = [];

            _.forEach(sockets, function (socket) {
                nodes.push(getNodeInfo(socket.uuid));
            })

            return {
                upTime: upTime,
                nodes: nodes
            }
        },

        start = function (next) {

            server
                .on('error', function (e) {
                    if (next)
                        next(e);
                })

                .listen(
                    options.port,
                    options.host,
                    function () {

                        // when were we started?
                        upSince = new Date();

                        if (next)
                            next(null);
                });

            return this;
        },

        stop= function() {

            // disconnect any connected clients
            _.forEach(sockets, function (socket) {
                socket.socket.end();
            })

            upSince = null;

            server.close();

            return this;
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
                if (node.authenticator.isAuthenticated())
                    node.socket.write(payload);
            })

            // return the message ID
            return messageId;
        },

        requestTimeoutExpired = function (pendingRequest) {

            delete pendingRequests[pendingRequest.messageId];

            // make sure we have a callback
            if (!pendingRequest.callback)
                return;

            // send what we have so far
            pendingRequest.callback("Timeout Expired", getRequestResponse(pendingRequest));
        },

        request = function (
            shardId,
            message,
            content,
            callback
        ) {

            // send the message
            var messageId = send(shardId, message, content),
                toSendNodes = nodeSelector.getKeyNodes(shardId),
                timeout = options.requestTimeout;

            var pendingRequest = {
                sent: new Date(),
                replies: {},
                callback: callback,
                timeout: null,
                messageId: messageId
            };

            // do we have a timeout?
            if (timeout != -1) {
                pendingRequest.timeout = setTimeout(requestTimeoutExpired, timeout, pendingRequest);
            }

            _.forEach(toSendNodes, function (node) {

                if (!node.authenticator.isAuthenticated())
                    return;

                pendingRequest.replies[node.uuid] = {
                    received: null // not yet received
                };
            });

            pendingRequests[messageId] = pendingRequest;
        },

        getNodeInfo = function (nodeId) {

            if (!_.has(sockets, nodeId))
                return null;

            var socket = sockets[nodeId];

            return {
                name: socket.name,
                upTime: new Date() - socket.upSince,
                upSince: socket.upSince,
                remoteAddress: socket.remoteAddress,
                id: socket.uuid
            }
        },

        getRequestResponse = function (pendingRequest) {

            var response = {
                responseTime: new Date() - pendingRequest.sent,
                replies: []
            };

            _.forEach(pendingRequest.replies, function (val, key) {

                if (!val.received)
                    return;

                var nodeResponse = {
                    node: getNodeInfo(key),
                    reply: val.reply,
                    responseTime: new Date() - val.received
                }

                response.replies.push(nodeResponse);
            });

            return response;
        },

        processMessages = function () {

            // process the message(s)
            _.forEach(sockets, function (socketInfo) {

                while (socketInfo.parser.hasMessage()) {

                    var msg = socketInfo.parser.nextMessage();

                    if (msg == null) {
                        socketInfo.socket.end();
                        return;
                    }

                    if (!socketInfo.authenticator.isAuthenticated()) {
                        socketInfo.authenticator.processMessage(msg);
                        continue;
                    }

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

                        // clear the timeout
                        if (pendingRequest.timeout)
                            clearInterval(pendingRequest.timeout);

                        delete pendingRequests[msg.id];

                        // make sure we have a callback
                        if (!pendingRequest.callback)
                            continue;

                        pendingRequest.callback(null, getRequestResponse(pendingRequest));
                    }
                }
            })
        };

    var server= null;

    // create the server
    if (options.tls && options.tls.enabled){

        var tlsOptions = {
            key: fs.readFileSync(options.tls.key),
            cert: fs.readFileSync(options.tls.cert)
        };

        server = tls.createServer(tlsOptions, socketConnected);
    }
    else
        server = net.createServer(socketConnected);

    return {
        status: status,
        start: start,
        stop: stop,
        on: on,
        send: send,
        request: request,
        options: getOptions
    };
}

////////////////////////////////////////////////////////////////////////////
// Authenticator

var Authenticator = function () {

    var
        tokenizer = new MessageTokenizer(),
        localAuthenticated = null,
        remoteAuthenticated = null,
        authOptions = {}

    var
        startAuthentication = function (options) {

            authOptions = options;

            // send the welcome message with auth info
            var payload = tokenizer.serialize({
                isWelcome: true,
                name: authOptions.name,
                auth: authOptions.auth
            });

            authOptions.socket.write(payload);
        },

        checkResult = function () {

            // make sure we have some results
            if (localAuthenticated == null ||
                remoteAuthenticated == null)
                return;

            if (isAuthenticated()) {
                if (authOptions.onSuccess)
                    authOptions.onSuccess();
            }
            else {

                var msg = "";

                if (!localAuthenticated && remoteAuthenticated)
                    msg = "Local authentication failed";

                else if (localAuthenticated && !remoteAuthenticated)
                    msg = "Remote authentication failed";

                else
                    msg = "Local & remote authentication failed";

                if (authOptions.onFail)
                    authOptions.onFail(msg);

                // disconnect them
                authOptions.socket.end();
            }
        },

        processMessage = function (msg) {

            if (_.has(msg, "authenticated")) {
                remoteAuthenticated= msg.authenticated;
                checkResult();
                return;
            }

            var peerCert = null;

            if (_.isFunction(authOptions.socket.getPeerCertificate))
                peerCert = authOptions.socket.getPeerCertificate();

            var authInfo = {
                data: msg.auth,
                host: authOptions.socket.remoteAddress,
                name: msg.name,
                cert: peerCert
            }

            // Attempt to authenticate the node
            if (authOptions.onAuthenticate && _.isFunction(authOptions.onAuthenticate))
                localAuthenticated = authOptions.onAuthenticate(authInfo);
            else
                localAuthenticated= true;

            // tell the client we have or have not authenticated them
            var payload = tokenizer.serialize({
                isWelcome: true,
                authenticated: localAuthenticated
            });

            authOptions.socket.write(payload);

            checkResult();
        },

        isAuthenticated = function () {
            return localAuthenticated && remoteAuthenticated;
        }

    return {
        startAuthentication: startAuthentication,
        isAuthenticated: isAuthenticated,
        processMessage: processMessage
    }
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
            try {
                return JSON.parse(msg, jsonDeserializeHelper);
            }
            catch (ex) {
                // ignore it.. or maybe dump the connection?
                return null;
            }
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