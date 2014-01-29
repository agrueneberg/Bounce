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
                    callback({
                        name: "Bad Request",
                        statusCode: 400,
                        message: "Missing username or password."
                    }, null);
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
                                             // Convert _id to a string.
                                                user._id = user._id.toHexString();
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
            getCollection: function (collectionName, user, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            collection.find({
                                "ming.read": user._id
                            }).count(function (err, count) {
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
                                callback({
                                    name: "Bad Request",
                                    statusCode: 400,
                                    message: err.message
                                }, null);
                            }
                        }
                    });
                });
            },
            getDocument: function (collectionName, documentName, user, callback) {
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
                                         // Check if the user is allowed to see the resource.
                                            if (document.ming.read.indexOf(user._id) === -1) {
                                                callback({
                                                    name: "Forbidden",
                                                    statusCode: 403,
                                                    message: "You are not allowed to see this resource",
                                                }, null);
                                            } else {
                                                callback(null, document);
                                            }
                                        }
                                    }
                                });
                            } catch (err) {
                                callback({
                                    name: "Bad Request",
                                    statusCode: 400,
                                    message: err.message
                                }, null);
                            }
                        }
                    });
                });
            },
            getField: function (collectionName, documentName, fieldName, user, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        var id, fields;
                        if (err !== null) {
                            callback(err, null);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentName);
                             // Include metadata.
                                fields = {
                                    ming: 1
                                };
                                fields[fieldName] = 1;
                                collection.findOne({
                                    _id: id
                                }, {
                                    fields: fields
                                }, function (err, document) {
                                    if (err !== null) {
                                        callback(err, null);
                                    } else {
                                        if (document === null) {
                                            callback(null, null);
                                        } else {
                                         // Check if the user is allowed to see the resource.
                                            if (document.ming.read.indexOf(user._id) === -1) {
                                                callback({
                                                    name: "Forbidden",
                                                    statusCode: 403,
                                                    message: "You are not allowed to see this resource",
                                                }, null);
                                            } else {
                                                if (document.hasOwnProperty(fieldName) === false) {
                                                    callback(null, null);
                                                } else {
                                                    callback(null, document[fieldName]);
                                                }
                                            }
                                        }
                                    }
                                });
                            } catch (err) {
                                callback({
                                    name: "Bad Request",
                                    statusCode: 400,
                                    message: err.message
                                }, null);
                            }
                        }
                    });
                });
            },
            query: function (collectionName, query, options, user, callback) {
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
                                    callback(null, documents.filter(function (document) {
                                     // Only include resources the user is allowed to see.
                                        if (document.ming.read.indexOf(user._id) === -1) {
                                            return false;
                                        } else {
                                            return true;
                                        }
                                    }));
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
            insertDocument: function (collectionName, document, user, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        if (err !== null) {
                            callback(err, null);
                        } else {
                         // Prepare metadata object.
                            if (document.hasOwnProperty("ming") === false) {
                                document.ming = {};
                            }
                         // Give creator read permission.
                            if (document.ming.hasOwnProperty("read") === false) {
                                document.ming.read = [];
                            }
                            if (document.ming.read.indexOf(user._id) === -1) {
                                document.ming.read.push(user._id);
                            }
                         // Give creator write permission.
                            if (document.ming.hasOwnProperty("write") === false) {
                                document.ming.write = [];
                            }
                            if (document.ming.write.indexOf(user._id) === -1) {
                                document.ming.write.push(user._id);
                            }
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
            updateDocument: function (collectionName, documentName, update, user, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        var id;
                        if (err !== null) {
                            callback(err, false);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentName);
                             // Check if user has write access to resource.
                                collection.findOne({
                                    _id: id
                                }, {
                                    fields: {
                                        ming: 1
                                    }
                                }, function (err, document) {
                                    if (err !== null) {
                                        callback(err, false);
                                    } else {
                                        if (document === null) {
                                            callback(null, false);
                                        } else {
                                            if (document.ming.write.indexOf(user._id) === -1) {
                                                callback({
                                                    name: "Forbidden",
                                                    statusCode: 403,
                                                    message: "You are not allowed to modify this resource",
                                                }, false);
                                            } else {
                                                collection.update({
                                                    _id: id
                                                }, update, function (err, result) {
                                                    if (err !== null) {
                                                        callback(err, false);
                                                    } else {
                                                        callback(null, result === 1);
                                                    }
                                                });
                                            }
                                        }
                                    }
                                });
                            } catch (err) {
                                callback({
                                    name: "Bad Request",
                                    statusCode: 400,
                                    message: err.message
                                }, false);
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
                        callback({
                            name: "Bad Request",
                            statusCode: 400,
                            message: err.message
                        }, false);
                    }
                });
            },
            deleteDocument: function (collectionName, documentName, user, callback) {
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        var id;
                        if (err !== null) {
                            callback(err, false);
                        } else {
                            try {
                                id = new mongo.ObjectID(documentName);
                             // Check if user has write access to resource.
                                collection.findOne({
                                    _id: id
                                }, {
                                    fields: {
                                        ming: 1
                                    }
                                }, function (err, document) {
                                    if (err !== null) {
                                        callback(err, false);
                                    } else {
                                        if (document === null) {
                                            callback(null, false);
                                        } else {
                                            if (document.ming.write.indexOf(user._id) === -1) {
                                                callback({
                                                    name: "Forbidden",
                                                    statusCode: 403,
                                                    message: "You are not allowed to modify this resource",
                                                }, false);
                                            } else {
                                                collection.remove({
                                                    _id: id
                                                }, function (err, num) {
                                                    if (err !== null) {
                                                        callback(err, false);
                                                    } else {
                                                        callback(null, num !== 0);
                                                    }
                                                });
                                            }
                                        }
                                    }
                                });
                            } catch (err) {
                                callback({
                                    name: "Bad Request",
                                    statusCode: 400,
                                    message: err.message
                                }, false);
                            }
                        }
                    });
                });
            }
        };
    };

}());
