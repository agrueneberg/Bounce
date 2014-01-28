(function () {
    "use strict";

    var mongo, bcrypt;

    mongo = require("mongodb");
    bcrypt = require("bcrypt");

    module.exports = function (options) {
        var dataSource;
     // TODO: Check parameters.
        dataSource = options.dataSource;
        return {
            register: function (user, callback) {
                if (user.hasOwnProperty("username") === false || user.hasOwnProperty("password") === false) {
                    callback(new Error("Missing username or password."), null);
                } else {
                    dataSource.then(function (db) {
                        db.collection("ming.users", function (err, collection) {
                            if (err !== null) {
                                callback(err, null);
                            } else {
                                bcrypt.hash(user.password, 8, function (err, hash) {
                                    user.password = hash;
                                    collection.insert(user, function (err, documents) {
                                        if (err !== null) {
                                            callback(err, null);
                                        } else {
                                            callback(null, documents[0]._id.toHexString());
                                        }
                                    });
                                });
                            }
                        });
                    });
                }
            },
            authenticate: function (credentials, callback) {
                dataSource.then(function (db) {
                    db.collection("ming.users", function (err, collection) {
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            collection.findOne({
                                username: credentials.name
                            }, function (err, user) {
                                if (err !== null) {
                                    callback(err, null);
                                } else {
                                    if (user === null) {
                                        callback(null, null);
                                    } else {
                                        bcrypt.compare(credentials.pass, user.password, function (err, passwordCorrect) {
                                            if (passwordCorrect === true) {
                                                callback(null, user);
                                            } else {
                                                callback(null, null);
                                            }
                                        });
                                    }
                                }
                            });
                        }
                    });
                });
            },
            getCollections: function (callback) {
                dataSource.then(function (db) {
                    db.collectionNames({
                        namesOnly: true
                    }, function (err, collections) {
                        var names;
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            names = collections.map(function (collection) {
                             // Strip database name.
                                return collection.substring(collection.indexOf(".") + 1);
                            });
                            callback(null, names);
                        }
                    });
                });
            },
            getCollection: function (collectionName, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            collection.find().count(function (err, count) {
                                if (err !== null) {
                                    callback(err, null);
                                } else {
                                    callback(null, count);
                                }
                            });
                        }
                    });
                });
            },
            getFile: function (prefixName, fileName, callback) {
                dataSource.then(function (db) {
                    db.collection(prefixName + ".files", function (err, collection) {
                        var id;
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            try {
                                id = new mongo.ObjectID(fileName);
                             // Get metadata first.
                                collection.findOne({
                                    _id: id
                                }, function (err, document) {
                                    var grid;
                                    if (err !== null) {
                                        callback(err, null);
                                    } else {
                                        if (document === null) {
                                            callback(null, null);
                                        } else {
                                            grid = new mongo.Grid(db, prefixName);
                                            grid.get(id, function (err, file) {
                                                var contentType;
                                                if (err !== null) {
                                                    callback(err, null);
                                                } else {
                                                    if (document.contentType) {
                                                        contentType = document.contentType;
                                                    } else if (document.filename) {
                                                     // Guess file type from file extension.
                                                        contentType = document.filename.match(/(\.\w+)$/)[1];
                                                    } else {
                                                        contentType = "application/octet-stream";
                                                    }
                                                    callback(null, {
                                                        contentType: contentType,
                                                        file: file
                                                    });
                                                }
                                            });
                                        }
                                    }
                                });
                            } catch (err) {
                                callback(err, null);
                            }
                        }
                    });
                });
            },
            getDocument: function (collectionName, documentName, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        var id;
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentName);
                                collection.findOne({
                                    _id: id
                                }, function (err, document) {
                                    if (err !== null) {
                                        callback(err, null);
                                    } else {
                                        if (document === null) {
                                            callback(null, null);
                                        } else {
                                            callback(null, document);
                                        }
                                    }
                                });
                            } catch (err) {
                                callback(err, null);
                            }
                        }
                    });
                });
            },
            getField: function (collectionName, documentName, fieldName, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        var id, fields;
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentName);
                                fields = {};
                                fields[fieldName] = 1;
                                collection.findOne({
                                    _id: id
                                }, {
                                    fields: fields
                                }, function (err, document) {
                                    if (err !== null) {
                                        callback(err, null);
                                    } else {
                                        if (document === null || document.hasOwnProperty(fieldName) === false) {
                                            callback(null, null);
                                        } else {
                                            callback(null, document[fieldName]);
                                        }
                                    }
                                });
                            } catch (err) {
                                callback(err, null);
                            }
                        }
                    });
                });
            },
            query: function (collectionName, query, options, callback) {
                if (typeof options === "function") {
                    callback = options;
                    options = {};
                }
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            collection.find(query, options).toArray(function (err, documents) {
                                if (err !== null) {
                                    callback(err, null);
                                } else {
                                    callback(null, documents);
                                }
                            });
                        }
                    });
                });
            },
            insertFile: function (prefixName, contentType, file, callback) {
                dataSource.then(function (db) {
                    var grid;
                    grid = new mongo.Grid(db, prefixName);
                    grid.put(file, {
                        content_type: contentType
                    }, function (err, document) {
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            callback(null, document._id.toHexString());
                        }
                    });
                });
            },
            insertDocument: function (collectionName, document, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            collection.insert(document, function (err, documents) {
                                if (err !== null) {
                                    callback(err, null);
                                } else {
                                    callback(null, documents[0]._id.toHexString());
                                }
                            });
                        }
                    });
                });
            },
            updateDocument: function (collectionName, documentName, document, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        var id;
                        if (err !== null) {
                            callback(err);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentName);
                                collection.update({
                                    _id: id
                                }, document, function (err) {
                                    callback(err);
                                });
                            } catch (err) {
                                callback(err);
                            }
                        }
                    });
                });
            },
            deleteFile: function (prefixName, fileName, callback) {
                dataSource.then(function (db) {
                    var grid, id;
                    grid = new mongo.Grid(db, prefixName);
                    try {
                        id = new mongo.ObjectID(fileName);
                        grid.delete(id, function (err, flag) {
                            if (err !== null) {
                                callback(err, false);
                            } else {
                                callback(null, flag);
                            }
                        });
                    } catch (err) {
                        callback(err, false);
                    }
                });
            },
            deleteDocument: function (collectionName, documentName, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        var id;
                        if (err !== null) {
                            callback(err, false);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentName);
                                collection.remove({
                                    _id: id
                                }, function (err, num) {
                                    if (err !== null) {
                                        callback(err, false);
                                    } else {
                                        callback(null, num !== 0);
                                    }
                                });
                            } catch (err) {
                                callback(err, false);
                            }
                        }
                    });
                });
            }
        };
    };

}());
