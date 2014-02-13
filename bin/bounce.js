#!/usr/bin/env node

(function () {
    "use strict";

    var argv, express, corser, authParser, bodyParser, errors, dataSource, bounce, app, auth, mergeLinks;

    argv = require("yargs")
             .options("port", {
                 default: 27080,
                 describe: "Port"
             })
             .options("connection-string", {
                 default: "mongodb://localhost/bounce",
                 describe: "MongoDB Connection String for the default deployment"
             })
             .options("database-permissions", {
                 describe: "Database-level permissions"
             })
             .argv;
    express = require("express");
    corser = require("corser");
    authParser = require("basic-auth");
    bodyParser = require("raw-body");
    errors = require("../lib/errors");
    dataSource = require("../lib/data-source")(argv["connection-string"]);
    bounce = require("../lib/bounce")({
        dataSource: dataSource,
        databasePermissions: argv["database-permissions"]
    });

    app = express();

    auth = function (req, res, next) {
        var unauthorized, credentials;
        unauthorized = function () {
            res.setHeader("WWW-Authenticate", "Basic realm=\"Bounce\"");
            res.send(401, "Unauthorized");
        };
        credentials = authParser(req);
        if (credentials === undefined) {
            credentials = null;
        } else {
         // Reformat credentials.
            credentials.username = credentials.name;
            delete credentials.name;
            credentials.password = credentials.pass;
            delete credentials.pass;
        }
        bounce.authenticate(credentials, function (err, user) {
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
    };

    mergeLinks = function (doc, links) {
        var relations;
        relations = Object.keys(links);
        if (doc.hasOwnProperty("_links") === false && relations.length > 0) {
            doc._links = {};
        }
        relations.forEach(function (link) {
            doc._links[link] = links[link];
        });
    };

    app.configure(function () {

     // Handle CORS.
        app.use(corser.create({
            methods: corser.simpleMethods.concat(["DELETE", "PUT"]),
            requestHeaders: corser.simpleRequestHeaders.concat(["Authorization"]),
            responseHeaders: corser.simpleResponseHeaders.concat(["Link", "Location"])
        }));

     // Terminate CORS preflights.
        app.use(function (req, res, next) {
            if (req.method === "OPTIONS") {
                res.send(204);
            } else {
                next();
            }
        });

     // Deploy routes.
        app.use(app.router);

     // Handle missing pages.
        app.use(function (req, res, next) {
            next(new errors.NotFound());
        });

     // Handle errors (signature must not be changed).
        app.use(function (err, req, res, next) {
            if (err.hasOwnProperty("statusCode") === true) {
                res.send(err.statusCode, err.message);
            } else {
                console.error(err);
                res.send(500, "Internal Server Error");
            }
        });

    });

    app.get("/", auth, function (req, res, next) {
        bounce.getCollections(req.user, function (err, collections) {
            if (err !== null) {
                next(err);
            } else {
                res.format({
                    "application/hal+json": function () {
                        res.send({
                            _links: {
                                self: {
                                    href: req.path
                                },
                                governance: {
                                    href: "/.well-known/governance?resource=/"
                                }
                            },
                            _embedded: {
                                collections: collections.map(function (collection) {
                                    mergeLinks(collection, {
                                        self: {
                                            href: "/" + collection.name
                                        },
                                        governance: {
                                            href: "/.well-known/governance?resource=/" + collection.name
                                        }
                                    });
                                    return collection;
                                })
                            }
                        });
                    }
                });
            }
        });
    });
    app.get("/bounce.users", function (req, res, next) {
        bounce.getUsers(function (err, users) {
            if (err !== null) {
                next(err);
            } else {
                res.format({
                    "application/hal+json": function () {
                        res.send({
                            _links: {
                                self: {
                                    href: req.path
                                }
                            },
                            _embedded: {
                                users: users.map(function (user) {
                                    mergeLinks(user, {
                                        self: {
                                            href: "/bounce.users/" + user.username
                                        }
                                    });
                                    return user;
                                })
                            }
                        });
                    }
                });
            }
        });
    });
    app.get("/:collection", auth, function (req, res, next) {
        var collectionParam;
        collectionParam = req.params.collection;
        bounce.getCollection(collectionParam, req.user, function (err, collection) {
            if (err !== null) {
                next(err);
            } else {
                res.format({
                    "application/hal+json": function () {
                        mergeLinks(collection, {
                            self: {
                                href: req.path
                            },
                            governance: {
                                href: "/.well-known/governance?resource=" + req.path
                            }
                        });
                        res.send(collection);
                    }
                });
            }
        });
    });
    app.get("/.well-known/governance", auth, function (req, res, next) {
        if (req.query.hasOwnProperty("resource") === false) {
            next(new errors.BadRequest("Missing \"resource\" URL parameter."));
        } else {
            bounce.getPermissions(req.query.resource, req.user, function (err, permissions) {
                if (err !== null) {
                    next(err);
                } else {
                    res.format({
                        "application/hal+json": function () {
                            mergeLinks(permissions, {
                                self: {
                                    href: req.path + "?resource=" + req.query.resource
                                }
                            });
                            res.send(permissions);
                        }
                    });
                }
            });
        }
    });
    app.get("/:prefix.files/:file", auth, function (req, res, next) {
        var prefixParam, fileParam;
        prefixParam = req.params.prefix;
        fileParam = req.params.file;
        if (req.query.hasOwnProperty("binary") === true && req.query.binary === "1") {
            bounce.getFile(prefixParam, fileParam, req.user, function (err, file) {
                if (err !== null) {
                    next(err);
                } else {
                    res.type(file.contentType);
                    res.header("Link", [
                        "<" + req.path + ">; rel=\"self\"",
                        "</.well-known/governance?resource=" + req.path + ">; rel=\"governance\""
                    ]);
                    res.send(file.file);
                }
            });
        } else {
            next();
        }
    });
    app.get("/bounce.users/:user", function (req, res, next) {
        var userParam;
        userParam = req.params.user;
        bounce.getUser(userParam, function (err, user) {
            if (err !== null) {
                next(err);
            } else {
                res.format({
                    "application/hal+json": function () {
                        mergeLinks(user, {
                            self: {
                                href: req.path
                            }
                        });
                        res.send(user);
                    }
                });
            }
        });
    });
    app.get("/:collection/:document", auth, function (req, res, next) {
        var collectionParam, documentParam;
        collectionParam = req.params.collection;
        documentParam = req.params.document;
        bounce.getDocument(collectionParam, documentParam, req.user, function (err, document) {
            if (err !== null) {
                next(err);
            } else {
             // Filter file properties.
                if (collectionParam.lastIndexOf(".files") !== -1 && collectionParam.lastIndexOf(".files") === collectionParam.length - 6) {
                    document = {
                        _id: document._id,
                        size: document.length,
                        contentType: document.contentType
                    };
                }
                res.format({
                    "application/hal+json": function () {
                        mergeLinks(document, {
                            self: {
                                href: req.path
                            },
                            governance: {
                                href: "/.well-known/governance?resource=" + req.path
                            }
                        });
                     // Do not expose _id of document.
                        delete document._id;
                        res.send(document);
                    }
                });
            }
        });
    });
    app.get("/:collection/:document/:field", auth, function (req, res, next) {
        var collectionParam, documentParam, fieldParam;
        collectionParam = req.params.collection;
        documentParam = req.params.document;
        fieldParam = req.params.field;
        bounce.getField(collectionParam, documentParam, fieldParam, req.user, function (err, field) {
            var document;
            if (err !== null) {
                next(err);
            } else {
                document = {};
                document[fieldParam] = field;
                res.format({
                    "application/hal+json": function () {
                        mergeLinks(document, {
                            self: {
                                href: req.path
                            },
                            governance: {
                                href: "/.well-known/governance?resource=/" + collectionParam + "/" + documentParam
                            }
                        });
                        res.send(document);
                    }
                });
            }
        });
    });
    app.post("/", [auth, express.json()], function (req, res, next) {
        bounce.insertCollection(req.body, req.user, function (err, collectionName) {
            if (err !== null) {
                next(err);
            } else {
                res.location(req.body.name + "/" + collectionName);
                res.send(201, "Created");
            }
        });
    });
    app.post("/:collection/query", [auth, express.json()], function (req, res, next) {
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
        bounce.query(collectionParam, query, options, req.user, function (err, documents) {
            if (err !== null) {
                next(err);
            } else {
             // Filter file properties.
                if (collectionParam.lastIndexOf(".files") !== -1 && collectionParam.lastIndexOf(".files") === collectionParam.length - 6) {
                    documents = documents.map(function (document) {
                        return {
                            _id: document._id,
                            size: document.length,
                            contentType: document.contentType
                        };
                    });
                }
                res.format({
                    "application/hal+json": function () {
                        res.send({
                            _links: {
                                self: {
                                    href: req.path
                                },
                                governance: {
                                    href: "/.well-known/governance?resource=/" + collectionParam
                                }
                            },
                            _embedded: {
                                results: documents.map(function (document) {
                                    mergeLinks(document, {
                                        self: {
                                            href: "/" + collectionParam + "/" + document._id
                                        },
                                        governance: {
                                            href: "/.well-known/governance?resource=/" + collectionParam + "/" + document._id
                                        }
                                    });
                                 // Do not expose _id of document.
                                    delete document._id;
                                    return document;
                                })
                            }
                        });
                    }
                });
            }
        });
    });
    app.post("/:prefix.files", auth, function (req, res, next) {
        bodyParser(req, function (err, buffer) {
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
            next(new errors.BadRequest("Empty body."));
        } else {
            bounce.insertFile(prefixParam, contentType, file, req.user, function (err, id) {
                if (err !== null) {
                    next(err);
                } else {
                    res.location(prefixParam + ".files/" + id);
                    res.send(201, "Created");
                }
            });
        }
    });
    app.post("/bounce.users", express.json(), function (req, res, next) {
        var user;
        user = req.body;
        bounce.register(user, function (err, id) {
            if (err !== null) {
                next(err);
            } else {
                res.location("bounce.users/" + id);
                res.send(201, "Created");
            }
        });
    });
    app.post("/:collection", [auth, express.json()], function (req, res, next) {
        var collectionParam, document;
        collectionParam = req.params.collection;
        document = req.body;
        bounce.insertDocument(collectionParam, document, req.user, function (err, id) {
            if (err !== null) {
                next(err);
            } else {
                res.location(collectionParam + "/" + id);
                res.send(201, "Created");
            }
        });
    });
    app.put("/:collection", [auth, express.json()], function (req, res, next) {
        var collectionParam, update;
        collectionParam = req.params.collection;
        update = req.body;
        bounce.updateCollection(collectionParam, update, req.user, function (err) {
            if (err !== null) {
                next(err);
            } else {
                res.send(204, "No Content");
            }
        });
    });
    app.put("/.well-known/governance", [auth, express.json()], function (req, res, next) {
        if (req.query.hasOwnProperty("resource") === false) {
            next(new errors.BadRequest("Missing \"resource\" URL parameter."));
        } else {
            bounce.updatePermissions(req.query.resource, req.body, req.user, function (err) {
                if (err !== null) {
                    next(err);
                } else {
                    res.send(204, "No Content");
                }
            });
        }
    });
    app.put("/:collection/:document", [auth, express.json()], function (req, res, next) {
        var collectionParam, documentParam, update;
        collectionParam = req.params.collection;
        documentParam = req.params.document;
        update = req.body;
        bounce.updateDocument(collectionParam, documentParam, update, req.user, function (err) {
            if (err !== null) {
                next(err);
            } else {
                res.send(204, "No Content");
            }
        });
    });
    app.delete("/:prefix.files/:file", auth, function (req, res, next) {
        var prefixParam, fileParam;
        prefixParam = req.params.prefix;
        fileParam = req.params.file;
        bounce.deleteFile(prefixParam, fileParam, req.user, function (err) {
            if (err !== null) {
                next(err);
            } else {
                res.send(200, "OK");
            }
        });
    });
    app.delete("/:collection", auth, function (req, res, next) {
        var collectionParam;
        collectionParam = req.params.collection;
        bounce.deleteCollection(collectionParam, req.user, function (err) {
            if (err !== null) {
                next(err);
            } else {
                res.send(200, "OK");
            }
        });
    });
    app.delete("/:collection/:document", auth, function (req, res, next) {
        var collectionParam, documentParam;
        collectionParam = req.params.collection;
        documentParam = req.params.document;
        bounce.deleteDocument(collectionParam, documentParam, req.user, function (err) {
            if (err !== null) {
                next(err);
            } else {
                res.send(200, "OK");
            }
        });
    });

    app.listen(argv.port);

    console.log("Bounce is running on port " + argv.port + ", connected to " + argv["connection-string"]);

}());
