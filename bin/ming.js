#!/usr/bin/env node

(function () {
    "use strict";

    var argv, express, corser, auth, rawBody, dataSource, ming, app;

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
    auth = require("basic-auth");
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
        app.use(function (req, res, next) {
            var unauthorized, credentials;
            unauthorized = function () {
                res.setHeader("WWW-Authenticate", "Basic realm=\"Ming\"");
                res.send(401, "Unauthorized");
            };
            credentials = auth(req);
            if (credentials === undefined) {
                unauthorized();
            } else {
                ming.authenticate(credentials, function (err, user) {
                    if (err !== null) {
                        next(err);
                    } else {
                        if (user === null) {
                            unauthorized();
                        } else {
                            req.user = user;
                            next();
                        }
                    }
                });
            }
        });

     // Deploy routes.
        app.use(app.router);

     // Handle missing pages.
        app.use(function (req, res) {
            res.send(404, "Not Found");
        });

     // Handle errors (signature must not be changed).
        app.use(function (err, req, res, next) {
            if (err.message.indexOf("a single String of 12 bytes or a string of 24 hex characters") != -1) {
                res.send(400, "Bad Request: Single String of 12 bytes or string of 24 hex characters expected");
            } else {
                res.send(500, "Internal Server Error");
            }
        });

    });

    app.get("/", function (req, res, next) {
        ming.getCollections(function (err, collections) {
            if (err !== null) {
                next(err);
            } else {
                res.send({
                    collections: collections
                });
            }
        });
    });
    app.get("/:collection", function (req, res, next) {
        var collectionParam;
        collectionParam = req.params.collection;
        ming.getCollection(collectionParam, function (err, count) {
            if (err !== null) {
                next(err);
            } else {
                res.send({
                    count: count
                });
            }
        });
    });
    app.get("/:prefix.files/:file", function (req, res, next) {
        var prefixParam, fileParam;
        prefixParam = req.params.prefix;
        fileParam = req.params.file;
        if (req.query.hasOwnProperty("binary") === true && req.query.binary === "true") {
            ming.getFile(prefixParam, fileParam, function (err, file) {
                if (err !== null) {
                    next(err);
                } else {
                    if (file === null) {
                        next();
                    } else {
                        res.type(file.contentType);
                        res.send(file.file);
                    }
                }
            });
        } else {
            ming.getDocument(prefixParam + ".files", fileParam, function (err, document) {
                if (err !== null) {
                    next(err);
                } else {
                    if (document === null) {
                        next();
                    } else {
                        res.send(document);
                    }
                }
            });
        }
    });
    app.get("/:collection/:document", function (req, res, next) {
        var collectionParam, documentParam;
        collectionParam = req.params.collection;
        documentParam = req.params.document;
        ming.getDocument(collectionParam, documentParam, function (err, document) {
            if (err !== null) {
                next(err);
            } else {
                if (document === null) {
                    next();
                } else {
                    res.send(document);
                }
            }
        });
    });
    app.get("/:collection/:document/:field", function (req, res, next) {
        var collectionParam, documentParam, fieldParam;
        collectionParam = req.params.collection;
        documentParam = req.params.document;
        fieldParam = req.params.field;
        ming.getField(collectionParam, documentParam, fieldParam, function (err, field) {
            if (err !== null) {
                next(err);
            } else {
                if (field === null) {
                    next();
                } else {
                    res.send(field);
                }
            }
        });
    });
    app.post("/:collection/query", express.json(), function (req, res, next) {
        var collectionParam, query, options;
        collectionParam = req.params.collection;
        query = req.body;
        options = {};
        if (req.query.limit) {
            options.limit = req.query.limit;
        }
        if (req.query.skip) {
            options.skip = req.query.skip;
        }
        if (req.query.sort) {
            options.sort = req.query.sort;
        }
        ming.query(collectionParam, options, function (err, documents) {
            if (err !== null) {
                next(err);
            } else {
                res.send(documents);
            }
        });
    });
    app.post("/:prefix.files", function (req, res, next) {
        rawBody(req, function (err, buffer) {
            if (err !== null) {
                next(err);
            } else {
                req.body = buffer;
                next();
            }
        });
    }, function (req, res, next) {
        var prefixParam, contentType, file;
        prefixParam = req.params.prefix;
        contentType = req.headers["content-type"];
        file = req.body;
     // Skip empty files.
        if (file.length === 0) {
            res.send(400, "Bad Request: Empty body");
        } else {
            ming.insertFile(prefixParam, contentType, file, function (err, id) {
                if (err !== null) {
                    next(err);
                } else {
                    res.location(prefixParam + ".files/" + id);
                    res.send(201, "Created");
                }
            });
        }
    });
    app.post("/:collection", express.json(), function(req, res, next) {
        var collectionParam, document;
        collectionParam = req.params.collection;
        document = req.body;
        ming.insertDocument(collectionParam, document, function (err, id) {
            if (err !== null) {
                next(err);
            } else {
                res.location(collectionParam + "/" + id);
                res.send(201, "Created");
            }
        });
    });
    app.patch("/:collection/:document", express.json(), function (req, res, next) {
        var collectionParam, documentParam, document;
        collectionParam = req.params.collection;
        documentParam = req.params.document;
        document = req.body;
        ming.updateDocument(collectionParam, documentParam, document, function (err) {
            if (err !== null) {
                next(err);
            } else {
                res.send(204, "No Content");
            }
        });
    });
    app.delete("/:prefix.files/:file", function (req, res, next) {
        var prefixParam, fileParam;
        prefixParam = req.params.prefix;
        fileParam = req.params.file;
        ming.deleteFile(prefixParam, fileParam, function (err, deleted) {
            if (err !== null) {
                next(err);
            } else {
                if (deleted === true) {
                    res.send(200, "OK");
                } else {
                    next();
                }
            }
        });
    });
    app.delete("/:collection/:document", function (req, res, next) {
        var collectionParam, documentParam;
        collectionParam = req.params.collection;
        documentParam = req.params.document;
        ming.deleteDocument(collectionParam, documentParam, function (err, deleted) {
            if (err !== null) {
                next(err);
            } else {
                if (deleted === true) {
                    res.send(200, "OK");
                } else {
                    next();
                }
            }
        });
    });

    app.listen(argv.port);

    console.log("Ming is running on port " + argv.port + ", connected to " + argv["connection-string"]);

}());
