
var expect = require("chai").expect,
    deasync = require('deasync'),
    _ = require("lodash");

describe("Test framework setup", function() {
    it("Make sure tests work", function(){
        expect(null).to.be.null();
    });
});

var AppSharder = require("../");

describe("Basic Tests", function() {
    it("Create a basic object", function(){

        expect(AppSharder).not.to.be.null();
    });

    it("Create a shard node without a master available", function(){

        // this should fail because we are not starting the master server

        var done = false;
        var startError = null;

        AppSharder.node({
            host: "127.0.0.1",
            port: 1233
        }, function (err) {
            startError= err;
            done= true;
        });

        while (!done)
            require('deasync').sleep(10);

        expect(startError).not.to.be.null();
    });

    it("Create a master & node and connect them together", function(){

        // this should fail because we are not starting the master server

        var done = false,
            masterError = null,
            nodeError = null

        var master = AppSharder.master({
            port: 1234
        }, function (err) {
            masterError= err;
            done= true;
        });

        while (!done)
            require('deasync').sleep(10);

        done = false;

        var node = AppSharder.node({
            port: 1234,
            host: "127.0.0.1"
        }, function (err) {
            nodeError= err;
            done= true;
        });

        while (!done)
            require('deasync').sleep(10);

        // we shoud have connected OK
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
        }, function (err) {
            masterError= err;
            done= true;
        });

        while (!done)
            require('deasync').sleep(10);

        done = false;

        var node = AppSharder.node({
            port: 1234,
            host: "127.0.0.1"
        }, function (err) {
            nodeError= err;
            done= true;
        });

        while (!done)
            require('deasync').sleep(10);

        // we shoud have connected OK
        expect(masterError).to.be.null();
        expect(nodeError).to.be.null();

        var status = master.status();

        expect(status.nodeCount).to.equal(1);

        // disconnect the client
        node.disconnect();

        // wait for the disconnect event to happen
        require('deasync').sleep(100);

        // get the status again
        var status = master.status();
        expect(status.nodeCount).to.equal(0);

        master.stop();
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

        startMaster = function () {

            var done = false,
                error = null;

            var master = AppSharder.master({
                port: 1234
            }, function (err) {
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
                }, function (err) {
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

            expect(status.nodeCount).to.equal(nodeCount);
        },

        delayWait = function (timeout, callback) {

            var start = new Date();

            while (!callback()) {

                if ((new Date()) - start > timeout)
                    throw "Timeout expired";

                require('deasync').sleep(10);
            }
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

    it("Request data from a single node", function () {

        var master = startMaster(),
            nodeCount = 25,
            nodeReply = "";

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

                console.log(reply.responseTime);
            }
        );

        delayWait(1000, function () {
            return nodeReply == "Some Data";
        });

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