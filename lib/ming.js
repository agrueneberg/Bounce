(function () {
    "use strict";

    var mongo, auth, bcrypt;

    mongo = require("mongodb");
    auth = require("basic-auth");
    bcrypt = require("bcrypt");

    module.exports = function (dataSource) {
        return {
            authenticate: function (req, res, next) {
                var unauthorized, credentials;
                unauthorized = function () {
                    res.setHeader("WWW-Authenticate", "Basic realm=\"Ming\"");
                    res.send(401, "Unauthorized");
                };
                credentials = auth(req);
                if (credentials === undefined) {
                    unauthorized();
                } else {
                    dataSource.then(function (db) {
                        db.collection("users", function (err, collection) {
                            if (err !== null) {
                                next(err);
                            } else {
                                collection.findOne({
                                    username: credentials.name
                                }, function (err, user) {
                                    if (err !== null || user === null) {
                                        unauthorized();
                                    } else {
                                        bcrypt.compare(credentials.pass, user.password, function (err, passwordCorrect) {
                                            if (passwordCorrect === true) {
                                                req.user = user;
                                                next();
                                            } else {
                                                unauthorized();
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    });
                }
            },
            getCollections: function (req, res, next) {
                dataSource.then(function (db) {
                    db.collectionNames({
                        namesOnly: true
                    }, function (err, collections) {
                        var names;
                        if (err !== null) {
                            next(err);
                        } else {
                            names = collections.map(function (collection) {
                             // Strip database name.
                                return collection.substring(collection.indexOf(".") + 1);
                            });
                            res.send({
                                collections: names
                            });
                        }
                    });
                });
            },
            getCollection: function (req, res, next) {
                var collectionParam;
                collectionParam = req.params.collection;
                dataSource.then(function (db) {
                    db.collection(collectionParam, function (err, collection) {
                        if (err !== null) {
                            next(err);
                        } else {
                            collection.find().count(function (err, count) {
                                if (err !== null) {
                                    next(err);
                                } else {
                                    res.send({count: count});
                                }
                            });
                        }
                    });
                });
            },
            getFile: function (req, res, next) {
                var prefixParam, fileParam;
                prefixParam = req.params.prefix;
                fileParam = req.params.file;
                dataSource.then(function (db) {
                    db.collection(prefixParam + ".files", function (err, collection) {
                        var id;
                        if (err !== null) {
                            next(err);
                        } else {
                            try {
                                id = new mongo.ObjectID(fileParam);
                             // Get metadata first.
                                collection.findOne({
                                    _id: id
                                }, function (err, document) {
                                    var grid;
                                    if (err !== null) {
                                        next(err);
                                    } else {
                                        if (document === null) {
                                            next();
                                        } else {
                                            if (req.query.hasOwnProperty("binary") === true && req.query.binary === "true") {
                                                grid = new mongo.Grid(db, prefixParam);
                                                grid.get(id, function (err, file) {
                                                    var contentType;
                                                    if (err !== null) {
                                                        next(err);
                                                    } else {
                                                        if (document.contentType) {
                                                            contentType = document.contentType;
                                                        } else if (document.filename) {
                                                         // Guess file type from file extension.
                                                            contentType = document.filename.match(/(\.\w+)$/)[1];
                                                        } else {
                                                            contentType = "application/octet-stream";
                                                        }
                                                        res.type(contentType);
                                                        res.send(file);
                                                    }
                                                });
                                            } else {
                                                res.send(document);
                                            }
                                        }
                                    }
                                });
                            } catch (e) {
                                next();
                            }
                        }
                    });
                });
            },
            getDocument: function (req, res, next) {
                var collectionParam, documentParam;
                collectionParam = req.params.collection;
                documentParam = req.params.document;
                dataSource.then(function (db) {
                    db.collection(collectionParam, function (err, collection) {
                        var id;
                        if (err !== null) {
                            next(err);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentParam);
                                collection.findOne({
                                    _id: id
                                }, function (err, document) {
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
                            } catch (e) {
                                next();
                            }
                        }
                    });
                });
            },
            getField: function (req, res, next) {
                var collectionParam, documentParam, fieldParam;
                collectionParam = req.params.collection;
                documentParam = req.params.document;
                fieldParam = req.params.field;
                dataSource.then(function (db) {
                    db.collection(collectionParam, function (err, collection) {
                        var id, fields;
                        if (err !== null) {
                            next(err);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentParam);
                                fields = {};
                                fields[fieldParam] = 1;
                                collection.findOne({
                                    _id: id
                                }, {
                                    fields: fields
                                }, function (err, document) {
                                    if (err !== null) {
                                        next(err);
                                    } else {
                                        if (document === null || document.hasOwnProperty(fieldParam) === false) {
                                            next();
                                        } else {
                                            res.send(document[fieldParam]);
                                        }
                                    }
                                });
                            } catch (e) {
                                next();
                            }
                        }
                    });
                });
            },
            query: function (req, res, next) {
                var collectionParam;
                collectionParam = req.params.collection;
                dataSource.then(function (db) {
                    db.collection(collectionParam, function (err, collection) {
                        var options;
                        if (err !== null) {
                            next(err);
                        } else {
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
                            collection.find(req.body, options).toArray(function (err, documents) {
                                if (err !== null) {
                                    next(err);
                                } else {
                                    res.send(documents);
                                }
                            });
                        }
                    });
                });
            },
            insertFile: function (req, res, next) {
                var prefixParam, contentType, grid;
                prefixParam = req.params.prefix;
                contentType = req.headers["content-type"];
             // Skip empty files.
                if (req.body.length === 0) {
                    res.send(400, "Bad Request: Empty body");
                } else {
                    dataSource.then(function (db) {
                        grid = new mongo.Grid(db, prefixParam);
                        grid.put(req.body, {
                            content_type: contentType
                        }, function (err, document) {
                            if (err !== null) {
                                next(err);
                            } else {
                                res.location(prefixParam + ".files/" + document._id.toHexString());
                                res.send(201, "Created");
                            }
                        });
                    });
                }
            },
            insertDocument: function (req, res, next) {
                var collectionParam, payload;
                collectionParam = req.params.collection;
                payload = req.body;
                dataSource.then(function (db) {
                    db.collection(collectionParam, function (err, collection) {
                        if (err !== null) {
                            next(err);
                        } else {
                            collection.insert(payload, function (err, document) {
                                if (err !== null) {
                                    next(err);
                                } else {
                                    res.location(collectionParam + "/" + document[0]._id.toHexString());
                                    res.send(201, "Created");
                                }
                            });
                        }
                    });
                });
            },
            updateDocument: function (req, res, next) {
                var collectionParam, documentParam;
                collectionParam = req.params.collection;
                documentParam = req.params.document;
                dataSource.then(function (db) {
                    db.collection(collectionParam, function (err, collection) {
                        var id;
                        if (err !== null) {
                            next(err);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentParam);
                                collection.update({
                                    _id: id
                                }, req.body, function (err) {
                                    if (err !== null) {
                                        next(err);
                                    } else {
                                        res.send(204, "No Content");
                                    }
                                });
                            } catch (e) {
                                next();
                            }
                        }
                    });
                });
            },
            deleteFile: function (req, res, next) {
                var prefixParam, fileParam, grid, id;
                prefixParam = req.params.prefix;
                fileParam = req.params.file;
                dataSource.then(function (db) {
                    grid = new mongo.Grid(db, prefixParam);
                    try {
                        id = new mongo.ObjectID(fileParam);
                        grid.delete(id, function (err, flag) {
                            if (err !== null) {
                                next(err);
                            } else {
                             // TODO: Detect if file was actually deleted.
                                res.send(200, "OK");
                            }
                        });
                    } catch (e) {
                        next();
                    }
                });
            },
            deleteDocument: function (req, res, next) {
                var collectionParam, documentParam;
                collectionParam = req.params.collection;
                documentParam = req.params.document;
                dataSource.then(function (db) {
                    db.collection(collectionParam, function (err, collection) {
                        var id;
                        if (err !== null) {
                            next(err);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentParam);
                                collection.remove({
                                    _id: id
                                }, function (err, num) {
                                    if (err !== null) {
                                        next(err);
                                    } else {
                                        if (num === 0) {
                                            next();
                                        } else {
                                            res.send(200, "Deleted");
                                        }
                                    }
                                });
                            } catch (e) {
                                next();
                            }
                        }
                    });
                });
            }
        };
    };

}());
