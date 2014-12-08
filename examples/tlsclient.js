var AppSharder = require("../");

var node = AppSharder.node({
    port: 1234,
    host: "127.0.0.1",
    name: "FirstNode",
    tls: {
        enabled: false
    },
    onAuthenticate: function(auth) {
        return true;
    }
});

node.connect(function (err) {
    console.log("Connected: " + err);
});