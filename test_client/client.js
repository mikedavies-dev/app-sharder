var AppSharder = require("../"),
    nodeFT = require('node-ft'),
    idx = nodeFT();

var node = AppSharder.node({
    port: 1234,
    host: "127.0.0.1",
    messages: [
        {
            name: "index",
            handler: function (msg) {
                idx.index(
                    msg.body.id,
                    msg.body.content
                );
            }
        },
        {
            name: "search",
            handler: function (msg) {
                msg.reply(idx.search(msg.body));
            }
        }
    ]
});

node.connect(function (err) {
    if (err)
        console.log("Error connecting to master : " + err);
    else
        console.log("Connected to master server");
});