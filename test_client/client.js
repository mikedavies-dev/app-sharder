var AppSharder = require("../"),
    nodeFT = require('node-ft'),
    idx = nodeFT();

var node = AppSharder.node(
    {
        port: 1234,
        host: "127.0.0.1"
    },

    function (err) {
        if (err){
            console.log("Error connecting to master server : " + err);
            return;
        }

        console.log("Connected to master server");
    })

    .on("index", function (data) {
        idx.index(
            data.id,
            data.text
        );
    })

    .on("search", function (text) {
        this.reply(idx.search(text));
    });