(function () {
    "use strict";

    var errors, mongo, bcrypt, async, url, isSystemCollection, isFilesCollection, createObjectId,
        getUserId, sanitizeDocument, parseResourcePath;

    errors = require("./errors");
    mongo = require("mongodb");
    bcrypt = require("bcrypt");
    async = require("async");
    url = require("url");

    isSystemCollection = function (col) {
        if (col.indexOf("system.") === 0) {
            return true;
        } else if (col.indexOf("ming.") === 0) {
            return true;
        } else if (col.lastIndexOf(".chunks") !== -1 && col.lastIndexOf(".chunks") === col.length - 7) {
            return true;
        } else {
            return false;
        }
    };

    isFilesCollection = function (col) {
        if (col.lastIndexOf(".files") !== -1 && col.lastIndexOf(".files") === col.length - 6) {
            return true;
        } else {
            return false;
        }
    };

    createObjectId = function (hex, callback) {
        var id;
        try {
            id = new mongo.ObjectID(hex);
            callback(null, id);
        } catch (err) {
            callback(new errors.BadRequest(err.message));
        }
    };

    getUserId = function (user) {
        if (typeof user === "object") {
            return user._id;
        } else {
            return user;
        }
    };

    sanitizeDocument = function (doc) {
        if (doc.hasOwnProperty("_links") === true) {
            delete doc._links;
        }
        if (doc.hasOwnProperty("_embedded") === true) {
            delete doc._embedded;
        }
        if (doc.hasOwnProperty("_permissions") === true) {
            delete doc._permissions;
        }
    };

    parseResourcePath = function (resourcePath, callback) {
        var resourceMatcher;
     // Remove URL parameters and fragment.
        resourcePath = url.parse(resourcePath).pathname;
     // Extract collection and document name.
        resourceMatcher = resourcePath.match(/^(\/)(?:([^\/]+)(?:\/([^\/]+))?)?(.*)*$/);
        if (resourceMatcher === null) {
            callback(new errors.BadRequest("\"resource\" URL parameter should be a valid pathname that starts with \"/\"."));
        } else {
            if (resourceMatcher[1] === "/" && resourceMatcher[2] !== undefined && resourceMatcher[3] === undefined && resourceMatcher[4] === undefined) {
                callback(null, {
                    collectionName: resourceMatcher[2]
                });
            } else if (resourceMatcher[1] === "/" && resourceMatcher[2] !== undefined && resourceMatcher[3] !== undefined && resourceMatcher[4] === undefined) {
                callback(null, {
                    collectionName: resourceMatcher[2],
                    documentName: resourceMatcher[3]
                });
            } else {
                callback(new errors.NotFound());
            }
        }
    };

    module.exports = function (options) {

        var dataSource, getCollections, getCollection, insertCollection, updateCollection, query,
            getDocument, getField, insertDocument, updateDocument, deleteDocument, getFile,
            insertFile, deleteFile, getUsers, getUser, register, authenticate, getPermissions,
            updatePermissions, hasPermission;

        dataSource = options.dataSource;

        getCollections = function (user, callback) {
            dataSource.then(function (db) {
                db.collection("ming.collections", function (err, collection) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        collection.find().toArray(function (err, documents) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                async.filter(documents, function (document, callback) {
                                    hasPermission("read", document._permissions, user, function (err, flag) {
                                        if (err !== null) {
                                            callback(false);
                                        } else {
                                            callback(flag);
                                        }
                                    });
                                }, function (documents) {
                                    documents = documents.map(function (document) {
                                     // Remove _permissions from document.
                                        delete document._permissions;
                                        return document;
                                    });
                                    callback(null, documents);
                                });
                            }
                        });
                    }
                });
            });
        };

        getCollection = function (collectionName, user, callback) {
            if (isSystemCollection(collectionName) === true) {
                callback(new errors.Forbidden());
            } else {
                dataSource.then(function (db) {
                    db.collection("ming.collections", function (err, collection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            collection.findOne({
                                name: collectionName
                            }, function (err, document) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    if (document === null) {
                                        callback(new errors.NotFound());
                                    } else {
                                        hasPermission("read", document._permissions, user, function (err, flag) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (flag === false) {
                                                    callback(new errors.Forbidden());
                                                } else {
                                                 // Remove _permissions from document.
                                                    delete document._permissions;
                                                    callback(null, document);
                                                }
                                            }
                                        });
                                    }
                                }
                            });
                        }
                    });
                });
            }
        };

        insertCollection = function (body, user, callback) {
            var collectionName;
            if (body.hasOwnProperty("name") === false) {
                callback(new errors.BadRequest("Missing name attribute."));
            } else {
                collectionName = body.name;
                if (isSystemCollection(collectionName) === true) {
                    callback(new errors.Forbidden());
                } else {
                    dataSource.then(function (db) {
                     // Create collection.
                        db.createCollection(collectionName, {
                            strict: true
                        }, function (err) {
                            if (err !== null) {
                                callback(new errors.Conflict("Collection already exists."));
                            } else {
                             // Create collection metadata.
                                db.collection("ming.collections", function (err, collection) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        collection.insert({
                                            name: collectionName,
                                            _permissions: {
                                                govern: [getUserId(user)],
                                                read: [getUserId(user)],
                                                write: [getUserId(user)],
                                                add: [getUserId(user)]
                                            }
                                        }, function (err, documents) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                callback(null, documents[0].name);
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    });
                }
            }
        };

        updateCollection = function (collectionName, body, user, callback) {
            if (isSystemCollection(collectionName) === true) {
                callback(new errors.Forbidden());
            } else {
             // Overwrite name in case someone is cheating.
                body.name = collectionName;
                sanitizeDocument(body);
                dataSource.then(function (db) {
                 // Get collection metadata.
                    db.collection("ming.collections", function (err, collection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            collection.findOne({
                                name: collectionName
                            }, {
                                fields: {
                                    _permissions: 1
                                }
                            }, function (err, document) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    if (document === null) {
                                        callback(new errors.NotFound());
                                    } else {
                                        hasPermission("write", document._permissions, user, function (err, flag) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (flag === false) {
                                                    callback(new errors.Forbidden());
                                                } else {
                                                 // Restore _id.
                                                    body._id = document._id;
                                                 // Restore _permissions.
                                                    body._permissions = document._permissions;
                                                    collection.save(body, function (err) {
                                                        if (err !== null) {
                                                            callback(err);
                                                        } else {
                                                            callback(null);
                                                        }
                                                    });
                                                }
                                            }
                                        });
                                    }
                                }
                            });
                        }
                    });
                });
            }
        };

        query = function (collectionName, query, options, user, callback) {
            if (isSystemCollection(collectionName) === true) {
                callback(new errors.Forbidden());
            } else {
                if (typeof options === "function") {
                    callback = options;
                    options = {};
                }
                dataSource.then(function (db) {
                    db.collection(collectionName, function (err, collection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            collection.find(query, options).toArray(function (err, documents) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    async.filter(documents, function (document, callback) {
                                        var documentMetadata;
                                     // Permissions of files are stored in the metadata property.
                                        if (isFilesCollection(collectionName) === true) {
                                            documentMetadata = document.metadata;
                                        } else {
                                            documentMetadata = document;
                                        }
                                        hasPermission("read", documentMetadata._permissions, user, function (err, flag) {
                                            if (err !== null) {
                                                callback(false);
                                            } else {
                                                callback(flag);
                                            }
                                        });
                                    }, function (documents) {
                                        documents = documents.map(function (document) {
                                         // Remove _permissions from document.
                                            if (isFilesCollection(collectionName) === true) {
                                                delete document.metadata._permissions;
                                            } else {
                                                delete document._permissions;
                                            }
                                            return document;
                                        });
                                        callback(null, documents);
                                    });
                                }
                            });
                        }
                    });
                });
            }
        };

        getDocument = function (collectionName, documentName, user, callback) {
            if (isSystemCollection(collectionName) === true) {
                callback(new errors.Forbidden());
            } else {
                createObjectId(documentName, function (err, id) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        dataSource.then(function (db) {
                            db.collection(collectionName, function (err, collection) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    collection.findOne({
                                        _id: id
                                    }, function (err, document) {
                                        var permissions;
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (document === null) {
                                                callback(new errors.NotFound());
                                            } else {
                                             // Permissions of files are stored in the metadata property.
                                                if (isFilesCollection(collectionName) === true) {
                                                    permissions = document.metadata._permissions;
                                                } else {
                                                    permissions = document._permissions;
                                                }
                                                hasPermission("read", permissions, user, function (err, flag) {
                                                    if (err !== null) {
                                                        callback(err);
                                                    } else {
                                                        if (flag === false) {
                                                            callback(new errors.Forbidden());
                                                        } else {
                                                         // Remove _permissions from document.
                                                            if (isFilesCollection(collectionName) === true) {
                                                                delete document.metadata._permissions;
                                                            } else {
                                                                delete document._permissions;
                                                            }
                                                            callback(null, document);
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    });
                                }
                            });
                        });
                    }
                });
            }
        };

        getField = function (collectionName, documentName, fieldName, user, callback) {
            if (isSystemCollection(collectionName) === true) {
                callback(new errors.Forbidden());
            } else {
             // Lie about storing permissions in the document itself.
                if (fieldName === "_permissions") {
                    callback(new errors.NotFound());
                } else {
                    createObjectId(documentName, function (err, id) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            dataSource.then(function (db) {
                                db.collection(collectionName, function (err, collection) {
                                    var fields;
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                     // Include permissions.
                                        fields = {
                                            _permissions: 1
                                        };
                                        fields[fieldName] = 1;
                                        collection.findOne({
                                            _id: id
                                        }, {
                                            fields: fields
                                        }, function (err, document) {
                                            var permissions;
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (document === null) {
                                                    callback(new errors.NotFound());
                                                } else {
                                                 // Permissions of files are stored in the metadata property.
                                                    if (isFilesCollection(collectionName) === true) {
                                                        permissions = document.metadata._permissions;
                                                    } else {
                                                        permissions = document._permissions;
                                                    }
                                                    hasPermission("read", permissions, user, function (err, flag) {
                                                        if (err !== null) {
                                                            callback(err);
                                                        } else {
                                                            if (flag === false) {
                                                                callback(new errors.Forbidden());
                                                            } else {
                                                                if (document.hasOwnProperty(fieldName) === false) {
                                                                    callback(new errors.NotFound());
                                                                } else {
                                                                 // Lie about storing permissions in the files metadata.
                                                                    if (isFilesCollection(collectionName) === true && fieldName === "metadata") {
                                                                        delete document.metadata._permissions;
                                                                    }
                                                                    callback(null, document[fieldName]);
                                                                }
                                                            }
                                                        }
                                                    });
                                                }
                                            }
                                        });
                                    }
                                });
                            });
                        }
                    });
                }
            }
        };

        insertDocument = function (collectionName, body, user, callback) {
            if (isSystemCollection(collectionName) === true) {
                callback(new errors.Forbidden());
            } else {
                sanitizeDocument(body);
                dataSource.then(function (db) {
                 // Get collection metadata, or create it in case it doesn't exist yet.
                    db.collection("ming.collections", function (err, collectionsCollection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            collectionsCollection.findAndModify({
                                name: collectionName
                            }, [
                            ], {
                                "$setOnInsert": {
                                    name: collectionName,
                                    _permissions: {
                                        govern: [getUserId(user)],
                                        read: [getUserId(user)],
                                        write: [getUserId(user)],
                                        add: [getUserId(user)]
                                    }
                                }
                            }, {
                                upsert: true,
                                new: true
                            }, function (err, collectionMetadata) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    hasPermission("add", collectionMetadata._permissions, user, function (err, flag) {
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (flag === false) {
                                                callback(new errors.Forbidden());
                                            } else {
                                                db.collection(collectionName, function (err, collection) {
                                                    if (err !== null) {
                                                        callback(err);
                                                    } else {
                                                     // Generate permissions.
                                                        body._permissions = {
                                                            _links: {
                                                                inherit: {
                                                                    href: "/" + collectionName
                                                                }
                                                            }
                                                        };
                                                        collection.insert(body, function (err, documents) {
                                                            if (err !== null) {
                                                                callback(err);
                                                            } else {
                                                                callback(null, documents[0]._id.toHexString());
                                                            }
                                                        });
                                                    }
                                                });
                                            }
                                        }
                                    });
                                }
                            });
                        }
                    });
                });
            }
        };

        updateDocument = function (collectionName, documentName, body, user, callback) {
            if (isSystemCollection(collectionName) === true) {
                callback(new errors.Forbidden());
            } else {
                createObjectId(documentName, function (err, id) {
                    if (err !== null) {
                        callback(err);
                    } else {
                     // Overwrite _id in case someone is cheating.
                        body._id = id;
                        sanitizeDocument(body);
                        dataSource.then(function (db) {
                            db.collection(collectionName, function (err, collection) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    collection.findOne({
                                        _id: id
                                    }, function (err, document) {
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (document === null) {
                                                callback(new errors.NotFound());
                                            } else {
                                                hasPermission("write", document._permissions, user, function (err, flag) {
                                                    if (err !== null) {
                                                        callback(err);
                                                    } else {
                                                        if (flag === false) {
                                                            callback(new errors.Forbidden());
                                                        } else {
                                                         // Restore _permissions.
                                                            body._permissions = document._permissions;
                                                            collection.save(body, function (err) {
                                                                if (err !== null) {
                                                                    callback(err);
                                                                } else {
                                                                    callback(null);
                                                                }
                                                            });
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    });
                                }
                            });
                        });
                    }
                });
            }
        };

        deleteDocument = function (collectionName, documentName, user, callback) {
            if (isSystemCollection(collectionName) === true) {
                callback(new errors.Forbidden());
            } else {
                createObjectId(documentName, function (err, id) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        dataSource.then(function (db) {
                            db.collection(collectionName, function (err, collection) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    collection.findOne({
                                        _id: id
                                    }, function (err, document) {
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (document === null) {
                                                callback(new errors.NotFound());
                                            } else {
                                                hasPermission("write", document._permissions, user, function (err, flag) {
                                                    if (err !== null) {
                                                        callback(err);
                                                    } else {
                                                        if (flag === false) {
                                                            callback(new errors.Forbidden());
                                                        } else {
                                                            collection.remove({
                                                                _id: id
                                                            }, function (err) {
                                                                if (err !== null) {
                                                                    callback(err);
                                                                } else {
                                                                    callback(null);
                                                                }
                                                            });
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    });
                                }
                            });
                        });
                    }
                });
            }
        };

        getFile = function (prefixName, fileName, user, callback) {
            var collectionName;
            collectionName = prefixName + ".files";
            createObjectId(fileName, function (err, id) {
                if (err !== null) {
                    callback(err);
                } else {
                    dataSource.then(function (db) {
                        db.collection(collectionName, function (err, collection) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                collection.findOne({
                                    _id: id
                                }, function (err, document) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        if (document === null) {
                                            callback(new errors.NotFound());
                                        } else {
                                            hasPermission("read", document._permissions, user, function (err, flag) {
                                                var grid;
                                                if (err !== null) {
                                                    callback(err);
                                                } else {
                                                    if (flag === false) {
                                                        callback(new errors.Forbidden());
                                                    } else {
                                                        grid = new mongo.Grid(db, prefixName);
                                                        grid.get(id, function (err, file) {
                                                            var contentType;
                                                            if (err !== null) {
                                                                callback(err);
                                                            } else {
                                                                if (document.contentType) {
                                                                    contentType = document.contentType;
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
                                        }
                                    }
                                });
                            }
                        });
                    });
                }
            });
        };

        insertFile = function (prefixName, contentType, file, user, callback) {
            var collectionName;
            collectionName = prefixName + ".files";
            dataSource.then(function (db) {
             // Get collection metadata, or create it in case it doesn't exist yet.
                db.collection("ming.collections", function (err, collectionsCollection) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        collectionsCollection.findAndModify({
                            name: collectionName
                        }, [
                        ], {
                            "$setOnInsert": {
                                name: collectionName,
                                _permissions: {
                                    govern: [getUserId(user)],
                                    read: [getUserId(user)],
                                    write: [getUserId(user)],
                                    add: [getUserId(user)]
                                }
                            }
                        }, {
                            upsert: true,
                            new: true
                        }, function (err, collectionMetadata) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                hasPermission("add", collectionMetadata._permissions, user, function (err, flag) {
                                    var grid;
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        if (flag === false) {
                                            callback(new errors.Forbidden());
                                        } else {
                                            grid = new mongo.Grid(db, prefixName);
                                            grid.put(file, {
                                                content_type: contentType,
                                                metadata: {
                                                    _permissions: {
                                                        _links: {
                                                            inherit: {
                                                                href: "/" + collectionName
                                                            }
                                                        }
                                                    }
                                                }
                                            }, function (err, document) {
                                                if (err !== null) {
                                                    callback(err);
                                                } else {
                                                    callback(null, document._id.toHexString());
                                                }
                                            });
                                        }
                                    }
                                });
                            }
                        });
                    }
                });
            });
        };

        deleteFile = function (prefixName, fileName, user, callback) {
            var collectionName;
            collectionName = prefixName + ".files";
            createObjectId(fileName, function (err, id) {
                if (err !== null) {
                    callback(err);
                } else {
                    dataSource.then(function (db) {
                        db.collection(collectionName, function (err, collection) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                collection.findOne({
                                    _id: id
                                }, function (err, document) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        if (document === null) {
                                            callback(new errors.NotFound());
                                        } else {
                                            hasPermission("write", document._permissions, user, function (err, flag) {
                                                var grid;
                                                if (err !== null) {
                                                    callback(err);
                                                } else {
                                                    if (flag === false) {
                                                        callback(new errors.Forbidden());
                                                    } else {
                                                        grid = new mongo.Grid(db, prefixName);
                                                        grid.delete(id, function (err) {
                                                            if (err !== null) {
                                                                callback(err);
                                                            } else {
                                                                callback(null);
                                                            }
                                                        });
                                                    }
                                                }
                                            });
                                        }
                                    }
                                });
                            }
                        });
                    });
                }
            });
        };

        getUsers = function (callback) {
            dataSource.then(function (db) {
                db.collection("ming.users", function (err, collection) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        collection.find({}, {
                            fields: {
                                password: 0
                            }
                        }).toArray(function (err, users) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                callback(null, users);
                            }
                        });
                    }
                });
            });
        };

        getUser = function (userName, callback) {
            createObjectId(userName, function (err, id) {
                if (err !== null) {
                    callback(err);
                } else {
                    dataSource.then(function (db) {
                        db.collection("ming.users", function (err, collection) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                collection.findOne({
                                    _id: id
                                }, {
                                    password: 0
                                }, function (err, user) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        if (user === null) {
                                            callback(new errors.NotFound());
                                        } else {
                                            callback(null, user);
                                        }
                                    }
                                });
                            }
                        });
                    });
                }
            });
        };

        register = function (user, callback) {
            if (user.hasOwnProperty("username") === false || user.hasOwnProperty("password") === false) {
                callback(new errors.BadRequest("Missing username or password attribute."));
            } else {
                dataSource.then(function (db) {
                    db.collection("ming.users", function (err, collection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            bcrypt.hash(user.password, 8, function (err, hash) {
                             // undefined instead of null for errors? Duh!
                                if (err !== undefined) {
                                    callback(err);
                                } else {
                                    user.password = hash;
                                    collection.ensureIndex({
                                        username: 1
                                    }, {
                                        unique: true
                                    }, function (err) {
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            collection.insert(user, function (err, documents) {
                                                if (err !== null) {
                                                    callback(new errors.Conflict("Username already exists."));
                                                } else {
                                                    callback(null, documents[0]._id.toHexString());
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                });
            }
        };

        authenticate = function (credentials, callback) {
            if (credentials === null) {
                callback(null, "public");
            } else {
                dataSource.then(function (db) {
                    db.collection("ming.users", function (err, collection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            collection.findOne({
                                username: credentials.username
                            }, function (err, user) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    if (user === null) {
                                        callback(null, null);
                                    } else {
                                        bcrypt.compare(credentials.password, user.password, function (err, passwordCorrect) {
                                         // undefined instead of null for errors? Duh!
                                            if (err !== undefined) {
                                                callback(err);
                                            } else {
                                                if (passwordCorrect === true) {
                                                 // Convert user ID to hex string. I wish we could keep ObjectIDs,
                                                 // but they are hard to deal with over HTTP.
                                                    user._id = user._id.toHexString();
                                                    callback(null, user);
                                                } else {
                                                    callback(null, null);
                                                }
                                            }
                                        });
                                    }
                                }
                            });
                        }
                    });
                });
            }
        };

        getPermissions = function (resourcePath, user, callback) {
            var resourceMatcher, collectionName, documentName;
            if (typeof user === "function" && callback === undefined) {
                callback = user;
                user = null;
            }
            parseResourcePath(resourcePath, function (err, names) {
                if (err !== null) {
                    callback(err);
                } else {
                    if (names.hasOwnProperty("collectionName") === true && names.hasOwnProperty("documentName") === false) {
                        collectionName = names.collectionName;
                        dataSource.then(function (db) {
                            db.collection("ming.collections", function (err, collectionsCollection) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    collectionsCollection.findOne({
                                        name: collectionName
                                    }, {
                                        fields: {
                                            _permissions: 1
                                        }
                                    }, function (err, collectionMetadata) {
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (collectionMetadata === null) {
                                                callback(new errors.NotFound());
                                            } else {
                                                if (user === null) {
                                                    callback(null, collectionMetadata._permissions);
                                                } else {
                                                    hasPermission("govern", collectionMetadata._permissions, user, function (err, flag) {
                                                        if (err !== null) {
                                                            callback(err);
                                                        } else {
                                                            if (flag === false) {
                                                                callback(new errors.Forbidden());
                                                            } else {
                                                                callback(null, collectionMetadata._permissions);
                                                            }
                                                        }
                                                    });
                                                }
                                            }
                                        }
                                    });
                                }
                            });
                        });
                    } else if (names.hasOwnProperty("collectionName") === true && names.hasOwnProperty("documentName") === true) {
                        collectionName = names.collectionName;
                        documentName = names.documentName;
                        createObjectId(documentName, function (err, id) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                dataSource.then(function (db) {
                                    db.collection(collectionName, function (err, collection) {
                                        var permissionsField;
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (isFilesCollection(collectionName) === true) {
                                                permissionsField = {
                                                    "metadata._permissions": 1
                                                };
                                            } else {
                                                permissionsField = {
                                                    "_permissions": 1
                                                };
                                            }
                                            collection.findOne({
                                                _id: id
                                            }, {
                                                fields: permissionsField
                                            }, function (err, document) {
                                                var permissions;
                                                if (err !== null) {
                                                    callback(err);
                                                } else {
                                                    if (document === null) {
                                                        callback(new errors.NotFound());
                                                    } else {
                                                     // Permissions of files are stored in the metadata property.
                                                        if (isFilesCollection(collectionName) === true) {
                                                            permissions = document.metadata._permissions;
                                                        } else {
                                                            permissions = document._permissions;
                                                        }
                                                        if (user === null) {
                                                            callback(null, permissions);
                                                        } else {
                                                            hasPermission("govern", permissions, user, function (err, flag) {
                                                                if (err !== null) {
                                                                    callback(err);
                                                                } else {
                                                                    if (flag === false) {
                                                                        callback(new errors.Forbidden());
                                                                    } else {
                                                                        callback(null, permissions);
                                                                    }
                                                                }
                                                            });
                                                        }
                                                    }
                                                }
                                            });
                                        }
                                    });
                                });
                            }
                        });
                    }
                }
            });
        };

        updatePermissions = function (resourcePath, body, user, callback) {
            var resourceMatcher, collectionName, documentName;
            parseResourcePath(resourcePath, function (err, names) {
                if (err !== null) {
                    callback(err);
                } else {
                    if (names.hasOwnProperty("collectionName") === true && names.hasOwnProperty("documentName") === false) {
                        collectionName = names.collectionName;
                        dataSource.then(function (db) {
                            db.collection("ming.collections", function (err, collectionsCollection) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    collectionsCollection.findOne({
                                        name: collectionName
                                    }, {
                                        fields: {
                                            _permissions: 1
                                        }
                                    }, function (err, collectionMetadata) {
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (collectionMetadata === null) {
                                                callback(new errors.NotFound());
                                            } else {
                                                hasPermission("govern", collectionMetadata._permissions, user, function (err, flag) {
                                                    if (err !== null) {
                                                        callback(err);
                                                    } else {
                                                        if (flag === false) {
                                                            callback(new errors.Forbidden());
                                                        } else {
                                                            collectionsCollection.update({
                                                                name: collectionName
                                                            }, {
                                                                $set: {
                                                                    _permissions: body
                                                                }
                                                            }, function (err) {
                                                                if (err !== null) {
                                                                    callback(err);
                                                                } else {
                                                                    callback(null);
                                                                }
                                                            });
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    });
                                }
                            });
                        });
                    } else if (names.hasOwnProperty("collectionName") === true && names.hasOwnProperty("documentName") === true) {
                        collectionName = names.collectionName;
                        documentName = names.documentName;
                        createObjectId(documentName, function (err, id) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                dataSource.then(function (db) {
                                    db.collection(collectionName, function (err, collection) {
                                        var permissionsField;
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (isFilesCollection(collectionName) === true) {
                                                permissionsField = {
                                                    "metadata._permissions": 1
                                                };
                                            } else {
                                                permissionsField = {
                                                    "_permissions": 1
                                                };
                                            }
                                            collection.findOne({
                                                _id: id
                                            }, {
                                                fields: permissionsField
                                            }, function (err, document) {
                                                var documentMetadata, instruction;
                                                if (err !== null) {
                                                    callback(err);
                                                } else {
                                                    if (document === null) {
                                                        callback(new errors.NotFound());
                                                    } else {
                                                     // Permissions of files are stored in the metadata property.
                                                        if (isFilesCollection(collectionName) === true) {
                                                            documentMetadata = document.metadata;
                                                        } else {
                                                            documentMetadata = document;
                                                        }
                                                        hasPermission("govern", documentMetadata._permissions, user, function (err, flag) {
                                                            if (err !== null) {
                                                                callback(err);
                                                            } else {
                                                                if (flag === false) {
                                                                    callback(new errors.Forbidden());
                                                                } else {
                                                                    if (isFilesCollection(collectionName) === true) {
                                                                        instruction = {
                                                                            $set: {
                                                                                "metadata._permissions": body
                                                                            }
                                                                        };
                                                                    } else {
                                                                        instruction = {
                                                                            $set: {
                                                                                _permissions: body
                                                                            }
                                                                        };
                                                                    }
                                                                    collection.update({
                                                                        _id: id
                                                                    }, instruction, function (err) {
                                                                        if (err !== null) {
                                                                            callback(err);
                                                                        } else {
                                                                            callback(null);
                                                                        }
                                                                    });
                                                                }
                                                            }
                                                        });
                                                    }
                                                }
                                            });
                                        }
                                    });
                                });
                            }
                        });
                    }
                }
            });
        };

        hasPermission = function (permission, permissions, user, callback) {
            async.whilst(function () {
                return !(permissions.hasOwnProperty(permission) === true && Array.isArray(permissions[permission]) === true);
            }, function (callback) {
                if (permissions.hasOwnProperty(permission) === false && permissions.hasOwnProperty("_links") === true && permissions._links.hasOwnProperty("inherit") === true && permissions._links.inherit.hasOwnProperty("href") === true && typeof permissions._links.inherit.href === "string") {
                    getPermissions(permissions._links.inherit.href, function (err, inheritedPermissions) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            permissions = inheritedPermissions;
                            callback();
                        }
                    });
                } else {
                    callback(new errors.Forbidden());
                }
            }, function (err) {
                if (err !== undefined) {
                    callback(err);
                } else {
                    callback(null, permissions[permission].indexOf(getUserId(user)) !== -1);
                }
            });
        };

        return {
            getCollections: getCollections,
            getCollection: getCollection,
            insertCollection: insertCollection,
            updateCollection: updateCollection,
            query: query,
            getDocument: getDocument,
            getField: getField,
            insertDocument: insertDocument,
            updateDocument: updateDocument,
            deleteDocument: deleteDocument,
            getFile: getFile,
            insertFile: insertFile,
            deleteFile: deleteFile,
            getUsers: getUsers,
            getUser: getUser,
            register: register,
            authenticate: authenticate,
            getPermissions: getPermissions,
            updatePermissions: updatePermissions
        };

    };

}());
