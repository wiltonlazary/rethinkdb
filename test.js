require("./build/packages/js")
.connect({"port":28022})
.then(function (conn) {
    return conn.server().then(function (res) {
        conn.close();
        return "success: " + JSON.stringify(res);
    });
})
.then(null, function (err) { return "error: " + JSON.stringify(err); })
.then(console.log);
