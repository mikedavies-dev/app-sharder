
var expect = require("chai").expect,
    deasync = require('deasync'),
    _ = require("lodash");

var AppSharder = require("../"),

    delayWait = function (timeout, callback) {

        var start = new Date();

        while (!callback()) {

            if ((new Date()) - start > timeout)
                throw "Timeout expired";

            require('deasync').sleep(10);
        }
    }

describe("Basic Tests", function() {

    it("Create a basic object", function(){

        expect(AppSharder).not.to.be.null();
    });

    it("Create a shard node without a master available", function(){

        // this should fail because we are not starting the master server

        var done = false,
            startError = null;
            node = AppSharder.node({
                host: "127.0.0.1",
                port: 1233
            });

        node.connect(function (err) {
            startError= err;
            done= true;
        });

        delayWait(2000, function () {
            return done;
        });

        expect(startError).not.to.be.null();
    });

    it("Create a master & node and connect them together", function(){

        // this should fail because we are not starting the master server

        var done = false,
            masterError = null,
            nodeError = null

        var master = AppSharder.master({
            port: 1234
        });

        master.start(function (err) {
            masterError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        done = false;

        var node = AppSharder.node({
            port: 1234,
            host: "127.0.0.1"
        });

        node.connect(function (err) {
            nodeError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        // we should have connected OK
        expect(masterError).to.be.null();
        expect(nodeError).to.be.null();

        master.stop();
        node.disconnect();
    });

    it("Create a master/node pair then disconnect the node", function(){

        // this should fail because we are not starting the master server

        var done = false,
            masterError = null,
            nodeError = null

        var master = AppSharder.master({
            port: 1234
        });

        master.start(function (err) {
            masterError= err;
            done= true;
        });

        while (!done)
            require('deasync').sleep(10);

        done = false;

        var node = AppSharder.node({
            port: 1234,
            host: "127.0.0.1",
            name: "FirstNode"
        });

        node.connect(function (err) {
            nodeError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        // we shoud have connected OK
        expect(masterError).to.be.null();
        expect(nodeError).to.be.null();

        var status = master.status();

        expect(status.nodes.length).to.equal(1);
        expect(status.nodes[0].name).to.equal("FirstNode");

        // disconnect the client
        node.disconnect();

        // wait for the disconnect event to happen
        require('deasync').sleep(100);

        // get the status again
        var status = master.status();
        expect(status.nodes.length).to.equal(0);

        // check up time
        expect(status.upTime).to.be.greaterThan(100);

        master.stop();
    });
});



describe("Handle node authentication", function () {
    it("Send auth data to the server, pass", function(){

        var done = false,
            masterError = null,
            nodeError = null

        var master = AppSharder.master({
            port: 1234,
            onAuthenticate: function (auth) {
                return auth.data.key == "some-auth-data";
            }
        });

        master.start(function (err) {
                masterError= err;
                done= true;
            });

        delayWait(1000, function () {
            return done;
        });

        done = false;

        var node = AppSharder.node({
            port: 1234,
            host: "127.0.0.1",
            name: "FirstNode",
            auth: {
                key: "some-auth-data",
                extra: "we can anything in the auth object"
            }
        });

        node.connect(function (err) {
            nodeError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        // we should have connected OK
        expect(masterError).to.be.null();
        expect(nodeError).to.be.null();

        var status = master.status();

        expect(status.nodes.length).to.equal(1);
        expect(status.nodes[0].name).to.equal("FirstNode");

        // disconnect the client
        node.disconnect();

        master.stop();
    });

    it("Send auth data to the server, fail", function(){

        var done = false,
            masterError = null,
            nodeError = null

        var master = AppSharder.master({
            port: 1234,
            onAuthenticate: function (auth) {
                return auth.data.key == "some-auth-data";
            }
        });

        master.start(function (err) {
            masterError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        done = false;

        var node = AppSharder.node({
            port: 1234,
            host: "127.0.0.1",
            name: "FirstNode",
            auth: {
                key: "we-dont-know-the-key",
                extra: "we can anything in the auth object"
            }
        });

        node.connect(function (err) {
            nodeError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        // we should have connected OK
        expect(masterError).to.be.null();
        expect(nodeError).to.equal("Remote authentication failed");

        var status = master.status();

        expect(status.nodes.length).to.equal(0);

        // disconnect the client
        node.disconnect();

        master.stop();
    });

    it("Send auth data to the client, pass", function(){

        var done = false,
            masterError = null,
            nodeError = null

        var master = AppSharder.master({
            port: 1234,
            auth: {
                key: "some-auth-data",
                extra: "we can anything in the auth object"
            }
        });

        master.start(function (err) {
            masterError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        done = false;

        var node = AppSharder.node({
            port: 1234,
            host: "127.0.0.1",
            name: "FirstNode",
            onAuthenticate: function (auth) {
                return auth.data.key == "some-auth-data";
            }
        });

        node.connect(function (err) {
            nodeError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        // we should have connected OK
        expect(masterError).to.be.null();
        expect(nodeError).to.be.null();

        var status = master.status();

        expect(status.nodes.length).to.equal(1);
        expect(status.nodes[0].name).to.equal("FirstNode");

        // disconnect the client
        node.disconnect();

        master.stop();
    });

    it("Send auth data to the client, fail", function(){

        var done = false,
            masterError = null,
            nodeError = null

        var master = AppSharder.master({
            port: 1234,
            auth: {
                key: "this-will-fail",
                extra: "we can anything in the auth object"
            }
        });

        master.start(function (err) {
            masterError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        done = false;

        var node = AppSharder.node({
            port: 1234,
            host: "127.0.0.1",
            name: "FirstNode",
            onAuthenticate: function (auth) {
                return auth.data.key == "some-auth-data";
            }
        });

        node.connect(function (err) {
            nodeError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        // we shouldn't have anything connected
        var status = master.status();
        expect(status.nodes.length).to.equal(0);

        // disconnect the client
        node.disconnect();

        master.stop();
    });
})

describe("Node and Master options", function() {

    it("Make sure passing null into options still creates null options", function () {

        var
            master = AppSharder.master(),
            options = master.options();

        expect(options).not.to.be.undefined();
        expect(options).not.to.be.null();
        expect(options.host).to.equal("0.0.0.0");
    });

    it("Default request timeout", function () {

        var
            master = AppSharder.master({
                port: 1234
            });

        var options = master.options();

        expect(options.port).to.equal(1234);
        expect(options.requestTimeout).to.equal(5000);
    });

    it("Default master port", function () {

        var master = AppSharder.master({});

        var options = master.options();

        expect(options.port).to.equal(5134);
    });

    it("Add two messages handlers via the node options", function () {

        var
            done = false,
            master = AppSharder.master();

        master.start(function (err) {
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        // now create the node
        done= false;

        var gotMessage1 = false,
            gotMessage2 = false,
            node = AppSharder.node({
                messages: [
                    {
                        name: "test-message1",
                        handler: function (msg) {
                            gotMessage1 = true;
                        }
                    },
                    {
                        name: "test-message2",
                        handler: function (msg) {
                            gotMessage2 = true;
                        }
                    }
                ]
            });

        node.connect(function (err) {
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        // send the messages
        master.send(null, "test-message1", "some message");
        master.send(null, "test-message2", "some message");

        delayWait(1000, function () {
            return gotMessage1 && gotMessage2;
        });
    });
});

describe("Message Tokenizer", function() {
    it("Serialize a message to text for sending", function () {

        var tokenizer = AppSharder.messageTokenizer();

        var toSend = tokenizer.serialize(
            "test-message",
            "test-body");

        expect(toSend).to.equal("\"test-message\"\u0000");
    });

    it("Deserialize some message text", function () {

        var parser = AppSharder.messageParser();

        parser.addData("\"test-message\"\u0000");

        expect(parser.hasMessage()).to.equal(true);

        var msg = parser.nextMessage();

        expect(msg).to.equal("test-message");
    });

    it("Add partial data to tokenizer, should not have message", function () {

        var parser = AppSharder.messageParser();

        parser.addData("\"test-me");

        expect(parser.hasMessage()).to.equal(false);

        parser.addData("ssage\"\u0000");

        expect(parser.hasMessage()).to.equal(true);
    });

    it("Add multiple messages for parsing", function () {

        var parser = AppSharder.messageParser();

        for (var i= 0; i< 10; i++) {
            parser.addData("\"" + i + "\"\u0000");
        }

        expect(parser.hasMessage()).to.equal(true);

        var checkIndex = 0;

        while (parser.hasMessage()) {
            var msg = parser.nextMessage();

            expect(msg).to.equal(String(checkIndex++));
        }

        // make sure we got 10 messages
        expect(checkIndex).to.equal(10);
    });

    it("Add partial data to tokenizer, then add some more", function () {

        var parser = AppSharder.messageParser();

        parser.addData("\"test-me");

        expect(parser.hasMessage()).to.equal(false);

        parser.addData("\"test-me");
    });

    it("Serialize / parse complex object", function () {

        var tokenizer = AppSharder.messageTokenizer(),
            parser = AppSharder.messageParser();

        var msg = tokenizer.serialize({
            id: "abcd-efg",
            count: 123,
            name: "do-some-searching",
            msg: {
                str1: "some text",
                arr1: [1, 2, 3, 4]
            }
        });

        parser.addData(msg);

        expect(parser.hasMessage()).to.equal(true);

        var msg2 = parser.nextMessage();

        expect(msg2)
            .to.have.property('id')
            .and.equal('abcd-efg');

        expect(msg2)
            .to.have.property('count')
            .and.equal(123);

        expect(msg2)
            .to.have.property('name')
            .and.equal("do-some-searching");
    });

    it("Serialize / parse a date", function () {

        var tokenizer = AppSharder.messageTokenizer(),
            parser = AppSharder.messageParser(),
            dateVal = new Date();

        var msg = tokenizer.serialize({
            sent: dateVal
        });

        parser.addData(msg);

        expect(parser.hasMessage()).to.equal(true);

        var msg2 = parser.nextMessage();

        expect(msg2.sent.getTime()).to.equal(dateVal.getTime());
    });
});

describe("Send a message to a single node", function() {

    var

        startMaster = function (options) {

            var done = false,
                error = null,
                defaultOptions = {
                    port: 1234
                };

            if (!options)
                options = {};

            options = _.defaults(options, defaultOptions);

            var master = AppSharder.master(options);

            master.start(function (err) {
                error= err;
                done= true;
            });

            delayWait(1000, function () {
                return done;
            });

            expect(error).to.be.null();

            return master;
        },

        connectNodes = function (master, nodeCount, events) {

            var done = false,
                error = null;

            for (var index= 0; index< nodeCount; index++) {

                done= false;

                var node = AppSharder.node({
                    port: 1234,
                    host: "127.0.0.1"
                });

                node.connect(function (err) {
                    error= err;
                    done= true;
                });

                delayWait(1000, function () {
                    return done;
                });

                expect(error).to.be.null();

                // add the events
                _.forEach(events, function (event) {
                    node.on(
                        event.message,
                        event.callback
                    );
                })
            }

            var status = master.status();

            expect(status.nodes.length).to.equal(nodeCount);
        }

    it("Send a message to a master that does not have nodes", function () {

        var master = startMaster();

        var sendMessage = function () {
            master.send(
                "abcd",
                "test-message",
                "some content"
            );
        };

        expect(sendMessage).to.throw("There are no nodes currently connected.");

        master.stop();
    });

    it("Send a message to a node", function () {

        var master = startMaster(),
            gotMessage = false;

        // connect the nodes
        connectNodes(master, 3, [
            {
                message: "test-message",
                callback: function (msg) {
                    gotMessage = true;
                }
            }
        ]);

        master.send(
            "abcd",
            "test-message",
            "some content"
        );

        delayWait(1000, function () {
            return gotMessage;
        });

        master.stop();
    });

    it("Send a message to all nodes", function () {

        var master = startMaster(),
            messageCount = 0,
            nodeCount = 3;

        // connect the nodes
        connectNodes(master, nodeCount, [
            {
                message: "test-message",
                callback: function (msg) {
                    messageCount++;
                }
            }
        ]);

        master.send(
            null,
            "test-message",
            "some content"
        );

        delayWait(1000, function () {
            return messageCount == nodeCount;
        });

        master.stop();
    });

    it("Request data from a all nodes", function () {

        var master = startMaster(),
            nodeCount = 25,
            nodeReply = null;

        // connect the nodes
        connectNodes(master, nodeCount, [
            {
                message: "test-message",
                callback: function (msg) {
                    msg.reply({
                        sent: new Date(),
                        num: 1234
                    });
                }
            }
        ]);

        master.request(
            null,
            "test-message",
            "some content",
            function (err, reply) {
                nodeReply = reply;
            }
        );

        delayWait(1000, function () {

            if (nodeReply == null)
                return false;

            return nodeReply.replies.length == nodeCount;
        });

        master.stop();
    });

    it("Request and cause a timeout", function () {

        var master = startMaster({
                requestTimeout: 100
            }),
            nodeCount = 1,
            nodeReply = null,
            nodeError = null;

        // connect the nodes
        connectNodes(master, nodeCount, [
            {
                message: "test-message",
                callback: function (msg) {

                    // sleep for 1 second
                    require('deasync').sleep(500);

                    msg.reply({
                        sent: new Date(),
                        num: 1234
                    });
                }
            }
        ]);

        master.request(
            null,
            "test-message",
            "some content",
            function (err, reply) {
                nodeReply = reply;
                nodeError = err;
            }
        );

        delayWait(1000, function () {

            if (nodeReply == null)
                return false;

            return nodeReply.replies != null;
        });

        expect(nodeReply.replies.length).to.equal(0);
        expect(nodeError).to.equal("Timeout Expired");

        master.stop();
    });
});

describe("Test the NodeSelector class", function() {

    it("Do we have this node?", function () {

        var selector = AppSharder.nodeSelector();

        selector.addNode("Server1Key", "Server1Value");

        expect(selector.hasNode("Server1Key")).to.equal(true);
    });

    it("Create a node selector and get a single node", function () {

        var selector = AppSharder.nodeSelector();

        selector.addNode("Server1Key", "Server1Value");
        selector.addNode("Server2", "Server2");
        selector.addNode("Server3", "Server3");
        selector.addNode("Server4", "Server4");

        var node = selector.getNode("Server1Key");

        expect(node).to.equal("Server1Value");
    });

    it("Select multiple nodes based on the id (null == all)", function () {

        var selector = AppSharder.nodeSelector();

        selector.addNode("Server1", "Server1");
        selector.addNode("Server2", "Server2");
        selector.addNode("Server3", "Server3");
        selector.addNode("Server4", "Server4");

        var nodes1 = selector.getKeyNodes("bacd");
        expect(nodes1.length).to.equal(1);

        var nodes2 = selector.getKeyNodes();
        expect(nodes2.length).to.equal(4);
    });

    it("Remove a node", function () {

        var selector = AppSharder.nodeSelector();

        selector.addNode("Server1", "Server1");
        selector.addNode("Server2", "Server2");
        selector.addNode("Server3", "Server3");
        selector.addNode("Server4", "Server4");

        var nodes1 = selector.getKeyNodes();
        expect(nodes1.length).to.equal(4);

        selector.removeNode("Server1");
        selector.removeNode("Server2");
        selector.removeNode("Server3");

        // we only have Server4 left so we should only ever get that one
        expect(selector.getKeyNodes("1")[0]).to.equal("Server4");
        expect(selector.getKeyNodes("2")[0]).to.equal("Server4");
        expect(selector.getKeyNodes("3")[0]).to.equal("Server4");
        expect(selector.getKeyNodes("4")[0]).to.equal("Server4");

        var nodes2 = selector.getKeyNodes();
        expect(nodes2.length).to.equal(1);
    });
});

/*

This does not appear to work for some reason.. connecting via TLS in the same thread/process
Does not connect..?

describe("Server TLS", function () {

    it ("Start a server using TLS", function () {

        var done = false,
            masterError = null,
            nodeError = null;

        var master = AppSharder.master({
            port: 1234,
            tls: {
                enabled: true,
                key: "./test/certs/master/private-key.pem",
                cert: "./test/certs/master/public-cert.pem"
            }
        });

        master.start(function (err) {
            masterError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        expect(masterError).to.be.null();

        var node = AppSharder.node({
            port: 1234,
            host: "127.0.0.1",
            name: "FirstNode",
            tls: {
                enabled: true
            }
        });

        node.connect(function (err) {
            nodeError= err;
            done= true;
        });

        delayWait(1000, function () {
            return done;
        });

        // we should have connected OK
        expect(masterError).to.be.null();
        expect(nodeError).to.be.null();

        var status = master.status();

        expect(status.nodes.length).to.equal(1);
        expect(status.nodes[0].name).to.equal("FirstNode");

        // disconnect the client
        node.disconnect();

        // wait for the disconnect event to happen
        require('deasync').sleep(100);

        // get the status again
        var status = master.status();
        expect(status.nodes.length).to.equal(0);

        // check up time
        expect(status.upTime).to.be.greaterThan(100);

        master.stop();
    });
});*/