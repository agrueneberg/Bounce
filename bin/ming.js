#!/usr/bin/env node

(function () {
    "use strict";

    var argv, express, corser, rawBody, dataSource, ming, app;

    argv = require("optimist")
             .options("port", {
                 default: 27080,
                 describe: "Port"
             })
             .options("connection-string", {
                 default: "mongodb://localhost/ming",
                 describe: "MongoDB Connection String for the default deployment."
             })
             .argv;
    express = require("express");
    corser = require("corser");
    rawBody = require("raw-body");
    dataSource = require("../lib/data-source")(argv["connection-string"]);
    ming = require("../lib/ming")({
        dataSource: dataSource
    });

    app = express();

    app.configure(function () {

     // Handle CORS.
        app.use(corser.create({
            methods: corser.simpleMethods.concat(["DELETE", "PATCH"]),
            requestHeaders: corser.simpleRequestHeaders.concat(["Authorization"]),
            responseHeaders: corser.simpleResponseHeaders.concat(["Location"])
        }));

     // Terminate CORS preflights.
        app.use(function (req, res, next) {
            if (req.method === "OPTIONS") {
                res.send(204);
            } else {
                next();
            }
        });

     // Authenticate user.
        app.use(ming.authenticate);

     // Deploy routes.
        app.use(app.router);

     // Handle missing pages.
        app.use(function (req, res) {
            res.send(404, "Not Found");
        });

     // Handle errors (signature must not be changed).
        app.use(function (err, req, res, next) {
            res.send(500, "Internal Server Error");
        });

    });

    app.get("/", ming.getCollections);
    app.get("/:collection", ming.getCollection);
    app.get("/:prefix.files/:file", ming.getFile);
    app.get("/:collection/:document", ming.getDocument);
    app.get("/:collection/:document/:field", ming.getField);
    app.post("/:collection/query", express.json(), ming.query);
    app.post("/:prefix.files", function (req, res, next) {
        rawBody(req, function (err, buffer) {
            if (err !== null) {
                next(err);
            } else {
                req.body = buffer;
                next();
            }
        });
    }, ming.insertFile);
    app.post("/:collection", express.json(), ming.insertDocument);
    app.patch("/:collection/:document", express.json(), ming.updateDocument);
    app.delete("/:prefix.files/:file", ming.deleteFile);
    app.delete("/:collection/:document", ming.deleteDocument);

    app.listen(argv.port);

    console.log("Ming is running on port " + argv.port + ", connected to " + argv["connection-string"]);

}());
