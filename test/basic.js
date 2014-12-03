
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
                callback: function () {
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
        })

        master.stop();
    });
});