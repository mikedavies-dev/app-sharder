var AppSharder = require("../");

var master = AppSharder.master({
    port: 1234,
    tls: {
        enabled: false,
        key: "../test/certs/master/private-key.pem",
        cert: "../test/certs/master/public-cert.pem"
    }
});

master.on("connect", function (socket) {
    console.log(socket.remoteAddress + " connected");
});

master.on("disconnect", function (socket) {
    console.log(socket.remoteAddress + " disconnected");
});

master.on("authenticated", function (socket) {
    console.log(socket.remoteAddress + " authenticated");
});

master.start(function (err) {
    masterError= err;
    done= true;
});