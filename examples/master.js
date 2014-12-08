var AppSharder = require("../"),
    readline= require('readline'),
    S = require('string');

var master = AppSharder.master(
    {
        port: 1234
    })

    .on("connect", function (client) {
        logEventMessage("Client " + client.remoteAddress + " connected");
    })

    .on("disconnect", function (client) {
        logEventMessage("Client disconnected");
    });

master.start(function (err) {

    if (err){
        logEventMessage("Error starting server : " + err);
        return;
    }

    logEventMessage("Server started");
});

var
    startCommandPrompt = function () {

        var rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });

        var onLine= function (line) {

            rl.question(commandPrompt(), onLine);

            var command = S(input).parseCSV(' ', "'");

            switch (command) {
                case "exit":
                    shutdownApp();
                    rl.close();
                    break;

                case "search":

                    var text = "this is some text from the user";

                    master.query("search", text, function (err, res) {
                        // do something with the results
                    });

                    break;

                case "index":

                    var id= "1234",
                        text = "this is some text from the user";

                    // sends a message to a single node based on shardHash(id)
                    master.send(
                        id,
                        "index",
                        text
                    );

                    // sends a message to all nodes
                    master.send(
                        null,
                        "index",
                        text
                    );

                    // requests from a single node
                    master.request(
                        id,
                        "index",
                        text,
                        function (err, res) {

                        }
                    );

                    // requests from a single node
                    master.request(
                        null,
                        "index",
                        text,
                        function (err, res) {

                        }
                    );

                    // master.send // send to a specific server based on id (hash)
                    // master.broadcast // broadcast and forget to all servers
                    // master.request // runs a query sent to all servers

                    master.update("index", id, text);
                    break;
            }
        }

        rl.question(commandPrompt(), onLine);
    },

    shutdownApp = function () {
        master.stop();
    },

    commandPrompt = function () {
        return "> ";
    }

    logEventMessage = function (msg) {
        console.log(msg);

        process.stdout.write(commandPrompt());
    };

    startCommandPrompt();