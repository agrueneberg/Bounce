(function () {
    "use strict";

    var errors, mongo, bcrypt, async, url, request, fs, tv4, isSystemCollection, isFilesCollection,
        createObjectId, getUserId, sanitizeDocument, parseResourceUri, validatePermissions;

    errors = require("./errors");
    mongo = require("mongodb");
    bcrypt = require("bcrypt");
    async = require("async");
    url = require("url");
    request = require("request");
    fs = require("fs");
    tv4 = require("tv4");

    isSystemCollection = function (col) {
        if (col.indexOf("system.") === 0) {
            return true;
        } else if (col.indexOf("bounce.") === 0) {
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
            return user.username;
        } else {
            return user;
        }
    };

    sanitizeDocument = function (doc) {
        if (doc.hasOwnProperty("_links") === true) {
            if (doc._links.hasOwnProperty("self") === true) {
                delete doc._links.self;
            }
            if (doc._links.hasOwnProperty("governance") === true) {
                delete doc._links.governance;
            }
        }
        if (doc.hasOwnProperty("_embedded") === true) {
            delete doc._embedded;
        }
        if (doc.hasOwnProperty("_creator") === true) {
            delete doc._creator;
        }
        if (doc.hasOwnProperty("_permissions") === true) {
            delete doc._permissions;
        }
    };

    parseResourceUri = function (resourceUri, callback) {
        var resourcePath, resourceMatcher;
        resourceUri = url.parse(resourceUri);
        if (resourceUri.protocol !== null && resourceUri.host !== null) {
            callback(null, {
                type: "external",
                href: resourceUri.href
            });
        } else {
         // Extract collection and document name from internal URI.
            resourceMatcher = resourceUri.pathname.match(/^\/(?:([^\/]+)(?:\/([^\/]+))?.*)?$/);
            if (resourceMatcher === null) {
                callback(new errors.BadRequest("\"resource\" URL parameter should be a valid pathname that starts with \"/\"."));
            } else {
                if (resourceMatcher[1] === undefined && resourceMatcher[2] === undefined) {
                    callback(null, {
                        type: "internal"
                    });
                } else if (resourceMatcher[1] !== undefined && resourceMatcher[2] === undefined) {
                    callback(null, {
                        type: "internal",
                        collectionName: resourceMatcher[1]
                    });
                } else if (resourceMatcher[1] !== undefined && resourceMatcher[2] !== undefined) {
                    callback(null, {
                        type: "internal",
                        collectionName: resourceMatcher[1],
                        documentName: resourceMatcher[2]
                    });
                } else {
                    callback(new errors.NotFound());
                }
            }
        }
    };

    validatePermissions = function (permissions) {
        var validationResult, operatorCount;
        validationResult = tv4.validateResult(permissions, {
            type: "object",
            properties: {
                _inherit: {
                    type: "string"
                },
                rules: {
                    type: "array",
                    items: {
                        type: "object",
                        properties: {
                            operator: {
                                type: "string",
                                enum: ["govern", "read", "write", "add"]
                            },
                            state: {
                                type: "string",
                                enum: ["all", "self", "none"]
                            }
                        },
                        required: ["operator", "state"],
                        oneOf: [{
                            type: "object",
                            properties: {
                                username: {
                                    type: "string"
                                }
                            },
                            required: ["username"]
                        }, {
                            type: "object",
                            properties: {
                                role: {
                                    type: "string",
                                    enum: ["authenticated", "public"]
                                }
                            },
                            required: ["role"]
                        }]
                    }
                }
            },
            anyOf: [{
                type: "object",
                required: ["_inherit"]
            }, {
                type: "object",
                required: ["rules"]
            }, {
                type: "object",
                required: ["_inherit", "rules"]
            }]
        }, false, true).valid;
        if (validationResult === true) {
            if (permissions.hasOwnProperty("_inherit") === false) {
             // Verify that every operator is at least defined once if no link is given.
                operatorCount = permissions.rules.reduce(function (previous, current) {
                    previous[current.operator] = previous[current.operator] + 1;
                    return previous;
                }, {
                    govern: 0,
                    read: 0,
                    write: 0,
                    add: 0
                });
                return Object.keys(operatorCount).every(function (operator) {
                    return operatorCount[operator] > 0;
                });
            } else {
                return true;
            }
        } else {
            return false;
        }
    };

    module.exports = function (options) {

        var dataSource, databasePermissions, getCollections, getCollection, insertCollection, updateCollection, deleteCollection,
            query, getDocument, getField, insertDocument, updateDocument, deleteDocument, getFile, insertFile, deleteFile,
            getUsers, getUser, register, authenticate, getPermissions, updatePermissions, hasPermission;

        dataSource = options.dataSource;

        if (options.databasePermissions !== undefined) {
            databasePermissions = JSON.parse(fs.readFileSync(options.databasePermissions));
        } else {
            databasePermissions = {
                "rules": [{
                    "operator": "govern",
                    "role": "authenticated",
                    "state": "self"
                }, {
                    "operator": "read",
                    "role": "authenticated",
                    "state": "self"
                }, {
                    "operator": "write",
                    "role": "authenticated",
                    "state": "self"
                }, {
                    "operator": "add",
                    "role": "authenticated",
                    "state": "self"
                }]
            };
        }
        getCollections = function (user, callback) {
            dataSource.then(function (db) {
                db.collection("bounce.collections", function (err, collection) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        collection.find().toArray(function (err, documents) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                async.filter(documents, function (document, callback) {
                                    hasPermission("read", document._permissions, document._creator, user, function (err, flag) {
                                        if (err !== null) {
                                            callback(false);
                                        } else {
                                            callback(flag);
                                        }
                                    });
                                }, function (documents) {
                                    documents = documents.map(function (document) {
                                     // Do not expose metadata of collection.
                                        delete document._id;
                                        delete document._creator;
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
                    db.collection("bounce.collections", function (err, collection) {
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
                                        hasPermission("read", document._permissions, document._creator, user, function (err, flag) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (flag === false) {
                                                    callback(new errors.Forbidden());
                                                } else {
                                                 // Do not expose metadata of collection.
                                                    delete document._id;
                                                    delete document._creator;
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
                     // TODO: Figure out if passing user as creator is harmful.
                        hasPermission("add", databasePermissions, getUserId(user), user, function (err, flag) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                if (flag === false) {
                                    callback(new errors.Forbidden());
                                } else {
                                 // Create collection.
                                    db.createCollection(collectionName, {
                                        strict: true
                                    }, function (err) {
                                        if (err !== null) {
                                            callback(new errors.Conflict("Collection already exists."));
                                        } else {
                                         // Create collection metadata.
                                            db.collection("bounce.collections", function (err, collection) {
                                                if (err !== null) {
                                                    callback(err);
                                                } else {
                                                    collection.insert({
                                                        name: collectionName,
                                                        _creator: getUserId(user),
                                                        _permissions: {
                                                            _inherit: "/"
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
                                }
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
                    db.collection("bounce.collections", function (err, collection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            collection.findOne({
                                name: collectionName
                            }, {
                                fields: {
                                    _creator: 1,
                                    _permissions: 1
                                }
                            }, function (err, document) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    if (document === null) {
                                        callback(new errors.NotFound());
                                    } else {
                                        hasPermission("write", document._permissions, document._creator, user, function (err, flag) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (flag === false) {
                                                    callback(new errors.Forbidden());
                                                } else {
                                                 // Restore metadata.
                                                    body._id = document._id;
                                                    body._creator = document._creator;
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

        deleteCollection = function (collectionName, user, callback) {
            if (isSystemCollection(collectionName) === true) {
                callback(new errors.Forbidden());
            } else {
                dataSource.then(function (db) {
                 // Get collection metadata.
                    db.collection("bounce.collections", function (err, collectionsCollection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            collectionsCollection.findOne({
                                name: collectionName
                            }, {
                                fields: {
                                    _creator: 1,
                                    _permissions: 1
                                }
                            }, function (err, document) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    if (document === null) {
                                        callback(new errors.NotFound());
                                    } else {
                                        hasPermission("write", document._permissions, document._creator, user, function (err, flag) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (flag === false) {
                                                    callback(new errors.Forbidden());
                                                } else {
                                                 // Delete collection metadata.
                                                    collectionsCollection.remove({
                                                        name: collectionName
                                                    }, function (err) {
                                                        if (err !== null) {
                                                            callback(err);
                                                        } else {
                                                         // Delete collection.
                                                            db.collection(collectionName, function (err, collection) {
                                                                collection.drop(function (err) {
                                                                    if (err !== null) {
                                                                        callback(err);
                                                                    } else {
                                                                        callback(null);
                                                                    }
                                                                });
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
                                        hasPermission("read", documentMetadata._permissions, documentMetadata._creator, user, function (err, flag) {
                                            if (err !== null) {
                                                callback(false);
                                            } else {
                                                callback(flag);
                                            }
                                        });
                                    }, function (documents) {
                                        documents = documents.map(function (document) {
                                         // Do not expose metadata of document.
                                            if (isFilesCollection(collectionName) === true) {
                                                delete document.metadata._creator;
                                                delete document.metadata._permissions;
                                            } else {
                                                delete document._creator;
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
                                        var documentMetadata;
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (document === null) {
                                                callback(new errors.NotFound());
                                            } else {
                                             // Metadata of files is stored in the metadata property.
                                                if (isFilesCollection(collectionName) === true) {
                                                    documentMetadata = document.metadata;
                                                } else {
                                                    documentMetadata = document;
                                                }
                                                hasPermission("read", documentMetadata._permissions, documentMetadata._creator, user, function (err, flag) {
                                                    if (err !== null) {
                                                        callback(err);
                                                    } else {
                                                        if (flag === false) {
                                                            callback(new errors.Forbidden());
                                                        } else {
                                                         // Do not expose _creator of document.
                                                            if (isFilesCollection(collectionName) === true) {
                                                                delete document.metadata._creator;
                                                                delete document.metadata._permissions;
                                                            } else {
                                                                delete document._creator;
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
                                            _creator: 1,
                                            _permissions: 1
                                        };
                                        fields[fieldName] = 1;
                                        collection.findOne({
                                            _id: id
                                        }, {
                                            fields: fields
                                        }, function (err, document) {
                                            var documentMetadata;
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (document === null) {
                                                    callback(new errors.NotFound());
                                                } else {
                                                 // Metadata of files is stored in the metadata property.
                                                    if (isFilesCollection(collectionName) === true) {
                                                        documentMetadata = document.metadata;
                                                    } else {
                                                        documentMetadata = document;
                                                    }
                                                    hasPermission("read", documentMetadata._permissions, documentMetadata._creator, user, function (err, flag) {
                                                        if (err !== null) {
                                                            callback(err);
                                                        } else {
                                                            if (flag === false) {
                                                                callback(new errors.Forbidden());
                                                            } else {
                                                                if (document.hasOwnProperty(fieldName) === false) {
                                                                    callback(new errors.NotFound());
                                                                } else {
                                                                 // Lie about storing creator and permissions in the files metadata.
                                                                    if (isFilesCollection(collectionName) === true && fieldName === "metadata") {
                                                                        delete document.metadata._creator;
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
                 // TODO: Figure out if passing user as creator is harmful.
                    hasPermission("add", databasePermissions, getUserId(user), user, function (err, flag) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            if (flag === false) {
                                callback(new errors.Forbidden());
                            } else {
                             // Get collection metadata, or create it in case it doesn't exist yet.
                                db.collection("bounce.collections", function (err, collectionsCollection) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        collectionsCollection.findAndModify({
                                            name: collectionName
                                        }, [
                                        ], {
                                            "$setOnInsert": {
                                                name: collectionName,
                                                _creator: getUserId(user),
                                                _permissions: {
                                                    _inherit: "/"
                                                }
                                            }
                                        }, {
                                            upsert: true,
                                            new: true
                                        }, function (err, collectionMetadata) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                hasPermission("add", collectionMetadata._permissions, collectionMetadata._creator, user, function (err, flag) {
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
                                                                    body._creator = getUserId(user);
                                                                    body._permissions = {
                                                                        _inherit: "/" + collectionName
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
                            }
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
                                                hasPermission("write", document._permissions, document._creator, user, function (err, flag) {
                                                    if (err !== null) {
                                                        callback(err);
                                                    } else {
                                                        if (flag === false) {
                                                            callback(new errors.Forbidden());
                                                        } else {
                                                         // Restore metadata.
                                                            body._creator = document._creator;
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
                                                hasPermission("write", document._permissions, document._creator, user, function (err, flag) {
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
                                            hasPermission("read", document.metadata._permissions, document.metadata._creator, user, function (err, flag) {
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
             // TODO: Figure out if passing user as creator is harmful.
                hasPermission("add", databasePermissions, getUserId(user), user, function (err, flag) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        if (flag === false) {
                            callback(new errors.Forbidden());
                        } else {
                         // Get collection metadata, or create it in case it doesn't exist yet.
                            db.collection("bounce.collections", function (err, collectionsCollection) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    collectionsCollection.findAndModify({
                                        name: collectionName
                                    }, [
                                    ], {
                                        "$setOnInsert": {
                                            name: collectionName,
                                            _creator: getUserId(user),
                                            _permissions: {
                                                _inherit: "/"
                                            }
                                        }
                                    }, {
                                        upsert: true,
                                        new: true
                                    }, function (err, collectionMetadata) {
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            hasPermission("add", collectionMetadata._permissions, collectionMetadata._creator, user, function (err, flag) {
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
                                                                _creator: getUserId(user),
                                                                _permissions: {
                                                                    _inherit: "/" + collectionName
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
                        }
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
                                            hasPermission("write", document.metadata._permissions, document.metadata._creator, user, function (err, flag) {
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
                db.collection("bounce.users", function (err, collection) {
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
                                users = users.map(function (user) {
                                 // Do not expose _id of user.
                                    delete user._id;
                                    return user;
                                });
                                callback(null, users);
                            }
                        });
                    }
                });
            });
        };

        getUser = function (username, callback) {
            dataSource.then(function (db) {
                db.collection("bounce.users", function (err, collection) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        collection.findOne({
                            username: username
                        }, {
                            fields: {
                                password: 0
                            }
                        }, function (err, user) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                if (user === null) {
                                    callback(new errors.NotFound());
                                } else {
                                 // Do not expose _id of user.
                                    delete user._id;
                                    callback(null, user);
                                }
                            }
                        });
                    }
                });
            });
        };

        register = function (user, callback) {
            if (user.hasOwnProperty("username") === false || user.hasOwnProperty("password") === false) {
                callback(new errors.BadRequest("Missing username or password attribute."));
            } else {
                if (user.username === "public" || user.username === "authenticated") {
                    callback(new errors.Conflict("The username \"" + user.username + "\" is not allowed."));
                } else {
                    dataSource.then(function (db) {
                        db.collection("bounce.users", function (err, collection) {
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
                                                        callback(null, documents[0].username);
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
            }
        };

        authenticate = function (credentials, callback) {
            if (credentials === null) {
                callback(null, {
                    username: "public"
                });
            } else {
                dataSource.then(function (db) {
                    db.collection("bounce.users", function (err, collection) {
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
                                                 // Do not expose _id of user.
                                                    delete user._id;
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
            parseResourceUri(resourcePath, function (err, parsed) {
                if (err !== null) {
                    callback(err);
                } else {
                    if (parsed.type === "external") {
                        request({
                            url: parsed.href,
                            json: true
                        }, function (err, response, body) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                if (user === null) {
                                    callback(null, body);
                                } else {
                                    callback(new errors.Forbidden());
                                }
                            }
                        });
                    } else {
                        if (parsed.hasOwnProperty("collectionName") === false && parsed.hasOwnProperty("documentName") === false) {
                            callback(null, databasePermissions);
                        } else if (parsed.hasOwnProperty("collectionName") === true && parsed.hasOwnProperty("documentName") === false) {
                            collectionName = parsed.collectionName;
                            dataSource.then(function (db) {
                                db.collection("bounce.collections", function (err, collectionsCollection) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        collectionsCollection.findOne({
                                            name: collectionName
                                        }, {
                                            fields: {
                                                _creator: 1,
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
                                                        hasPermission("govern", collectionMetadata._permissions, collectionMetadata._creator, user, function (err, flag) {
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
                        } else if (parsed.hasOwnProperty("collectionName") === true && parsed.hasOwnProperty("documentName") === true) {
                            collectionName = parsed.collectionName;
                            documentName = parsed.documentName;
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
                                                        "metadata._creator": 1,
                                                        "metadata._permissions": 1
                                                    };
                                                } else {
                                                    permissionsField = {
                                                        _creator: 1,
                                                        _permissions: 1
                                                    };
                                                }
                                                collection.findOne({
                                                    _id: id
                                                }, {
                                                    fields: permissionsField
                                                }, function (err, document) {
                                                    var documentMetadata;
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
                                                            if (user === null) {
                                                                callback(null, documentMetadata._permissions);
                                                            } else {
                                                                hasPermission("govern", documentMetadata._permissions, documentMetadata._creator, user, function (err, flag) {
                                                                    if (err !== null) {
                                                                        callback(err);
                                                                    } else {
                                                                        if (flag === false) {
                                                                            callback(new errors.Forbidden());
                                                                        } else {
                                                                            callback(null, documentMetadata._permissions);
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
                }
            });
        };

        updatePermissions = function (resourcePath, body, user, callback) {
            var resourceMatcher, collectionName, documentName;
            if (validatePermissions(body) === false) {
                callback(new errors.BadRequest("All four operators have to be defined with valid states unless the resource inherits from another resource."));
            } else {
                parseResourceUri(resourcePath, function (err, parsed) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        if (parsed.hasOwnProperty("collectionName") === true && parsed.hasOwnProperty("documentName") === false) {
                            collectionName = parsed.collectionName;
                            dataSource.then(function (db) {
                                db.collection("bounce.collections", function (err, collectionsCollection) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        collectionsCollection.findOne({
                                            name: collectionName
                                        }, {
                                            fields: {
                                                _creator: 1,
                                                _permissions: 1
                                            }
                                        }, function (err, collectionMetadata) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (collectionMetadata === null) {
                                                    callback(new errors.NotFound());
                                                } else {
                                                    hasPermission("govern", collectionMetadata._permissions, collectionMetadata._creator, user, function (err, flag) {
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
                        } else if (parsed.hasOwnProperty("collectionName") === true && parsed.hasOwnProperty("documentName") === true) {
                            collectionName = parsed.collectionName;
                            documentName = parsed.documentName;
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
                                                        "metadata._creator": 1,
                                                        "metadata._permissions": 1
                                                    };
                                                } else {
                                                    permissionsField = {
                                                        _creator: 1,
                                                        _permissions: 1
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
                                                            hasPermission("govern", documentMetadata._permissions, documentMetadata._creator, user, function (err, flag) {
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
            }
        };

        hasPermission = function (operator, permissions, creator, user, callback) {
            var matchingRules;
            async.whilst(function () {
             // Walk links if no rules have been defined.
                if (permissions.hasOwnProperty("rules") === false) {
                    return true;
                } else {
                 // Find matching operator/user pairs.
                    matchingRules = permissions.rules.filter(function (rule) {
                        return rule.operator === operator && (rule.username === getUserId(user) || rule.hasOwnProperty("role") === true);
                    });
                 // Walk links if no matching rules could be found.
                    return matchingRules.length === 0;
                }
            }, function (callback) {
                getPermissions(permissions._inherit, function (err, inheritedPermissions) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        if (validatePermissions(inheritedPermissions) === false) {
                            callback(new errors.BadRequest("All four operators have to be defined with valid states unless the resource inherits from another resource."));
                        } else {
                            permissions = inheritedPermissions;
                            callback();
                        }
                    }
                });
            }, function (err) {
                var flag;
                if (err !== undefined) {
                    callback(err);
                } else {
                    flag = matchingRules.some(function (rule) {
                     // Explicit access.
                        if (rule.hasOwnProperty("username") === true && (rule.state === "all" || (rule.state === "self" && getUserId(user) === creator))) {
                            return true;
                     // Authenticated access.
                        } else if (getUserId(user) !== "public" && rule.hasOwnProperty("role") === true && rule.role === "authenticated" && (rule.state === "all" || (rule.state === "self" && getUserId(user) === creator))) {
                            return true;
                     // Public access.
                        } else if (getUserId(user) === "public" && rule.hasOwnProperty("role") === true && rule.role === "public" && (rule.state === "all" || rule.state === "self")) {
                            return true;
                        } else {
                            return false;
                        }
                    });
                    callback(null, flag);
                }
            });
        };

        return {
            getCollections: getCollections,
            getCollection: getCollection,
            insertCollection: insertCollection,
            updateCollection: updateCollection,
            deleteCollection: deleteCollection,
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
