(function () {
    "use strict";

    var mongo, bcrypt, isSystemCollection, hasPermission, createObjectID, getUserId;

    mongo = require("mongodb");
    bcrypt = require("bcrypt");

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

    hasPermission = function (permission, resource, user) {
        return resource._permissions[permission].indexOf(getUserId(user)) !== -1;
    };

    createObjectID = function (hex, callback) {
        var id;
        try {
            id = new mongo.ObjectID(hex);
            callback(null, id);
        } catch (err) {
            callback({
                name: "Bad Request",
                statusCode: 400,
                message: err.message
            });
        }
    };

    getUserId = function (user) {
        if (typeof user === "object") {
            return user._id;
        } else {
            return user;
        }
    };

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
                    });
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
                                                        callback({
                                                            name: "Bad Request",
                                                            statusCode: 409,
                                                            message: "Username already exists."
                                                        });
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
            },
            authenticate: function (credentials, callback) {
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
            },
            getUsers: function (callback) {
                dataSource.then(function (db) {
                    db.collection("ming.users", function (err, collection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            collection.find({
                            }, {
                                password: 0
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
            },
            getUser: function (userName, callback) {
                createObjectID(userName, function (err, id) {
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
                                                callback(null, null);
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
            },
            getCollections: function (user, callback) {
                dataSource.then(function (db) {
                    db.collection("ming.collections", function (err, collection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            collection.find({
                                "_permissions.read": getUserId(user)
                            }, {
                                name: 1
                            }).toArray(function (err, collections) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    collections = collections.map(function (collection) {
                                        return collection.name;
                                    });
                                    callback(null, collections);
                                }
                            });
                        }
                    });
                });
            },
            getCollection: function (collectionName, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback({
                        name: "Forbidden",
                        statusCode: 403,
                        message: "You are not allowed to access this resource"
                    });
                } else {
                    dataSource.then(function (db) {
                        db.collection("ming.collections", function (err, collectionsCollection) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                collectionsCollection.findOne({
                                    name: collectionName
                                }, function (err, collectionMetadata) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        if (collectionMetadata === null) {
                                            callback(null, null);
                                        } else {
                                            if (hasPermission("read", collectionMetadata, user) === false) {
                                                callback({
                                                    name: "Forbidden",
                                                    statusCode: 403,
                                                    message: "You are not allowed to access this resource"
                                                });
                                            } else {
                                             // Do not expose ID.
                                                delete collectionMetadata._id;
                                                callback(null, collectionMetadata);
                                            }
                                        }
                                    }
                                });
                            }
                        });
                    });
                }
            },
            getFile: function (prefixName, fileName, user, callback) {
                createObjectID(fileName, function (err, id) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        dataSource.then(function (db) {
                            db.collection(prefixName + ".files", function (err, collection) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                 // Get metadata first.
                                    collection.findOne({
                                        _id: id
                                    }, function (err, document) {
                                        var grid;
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (document === null) {
                                                callback(null, null);
                                            } else {
                                                if (hasPermission("read", document.metadata, user) === false) {
                                                    callback({
                                                        name: "Forbidden",
                                                        statusCode: 403,
                                                        message: "You are not allowed to access this resource"
                                                    });
                                                } else {
                                                    grid = new mongo.Grid(db, prefixName);
                                                    grid.get(id, function (err, file) {
                                                        var contentType;
                                                        if (err !== null) {
                                                            callback(err);
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
                                        }
                                    });
                                }
                            });
                        });
                    }
                });
            },
            getDocument: function (collectionName, documentName, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback({
                        name: "Forbidden",
                        statusCode: 403,
                        message: "You are not allowed to access this resource"
                    });
                } else {
                    createObjectID(documentName, function (err, id) {
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
                                            var metadata;
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (document === null) {
                                                    callback(null, null);
                                                } else {
                                                 // Permissions of files are stored in the metadata property.
                                                    if (collectionName.lastIndexOf(".files") !== -1 && collectionName.lastIndexOf(".files") === collectionName.length - 6) {
                                                        metadata = document.metadata;
                                                    } else {
                                                        metadata = document;
                                                    }
                                                 // Check if the user is allowed to see the resource.
                                                    if (metadata._permissions.read === "inherit") {
                                                     // Read permissions are inherited from the collection.
                                                        db.collection("ming.collections", function (err, collectionsCollection) {
                                                            if (err !== null) {
                                                                callback(err);
                                                            } else {
                                                                collectionsCollection.findOne({
                                                                    name: collectionName
                                                                }, {
                                                                    "_permissions.read": 1
                                                                }, function (err, collectionMetadata) {
                                                                    if (err !== null) {
                                                                        callback(err);
                                                                    } else {
                                                                        if (collectionMetadata === null) {
                                                                            callback(null, null);
                                                                        } else {
                                                                            if (hasPermission("read", collectionMetadata, user) === false) {
                                                                                callback({
                                                                                    name: "Forbidden",
                                                                                    statusCode: 403,
                                                                                    message: "You are not allowed to access this resource"
                                                                                });
                                                                            } else {
                                                                                callback(null, document);
                                                                            }
                                                                        }
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    } else {
                                                     // Read permissions are explicitly defined.
                                                        if (hasPermission("read", metadata, user) === false) {
                                                            callback({
                                                                name: "Forbidden",
                                                                statusCode: 403,
                                                                message: "You are not allowed to access this resource"
                                                            });
                                                        } else {
                                                            callback(null, document);
                                                        }
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
            },
            getField: function (collectionName, documentName, fieldName, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback({
                        name: "Forbidden",
                        statusCode: 403,
                        message: "You are not allowed to access this resource"
                    });
                } else {
                    createObjectID(documentName, function (err, id) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            dataSource.then(function (db) {
                                db.collection(collectionName, function (err, collection) {
                                    var fields;
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                     // Include metadata.
                                        fields = {
                                            "_permissions.read": 1
                                        };
                                        fields[fieldName] = 1;
                                        collection.findOne({
                                            _id: id
                                        }, {
                                            fields: fields
                                        }, function (err, document) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (document === null) {
                                                    callback(null, null);
                                                } else {
                                                 // Check if the user is allowed to see the resource.
                                                    if (document._permissions.read === "inherit") {
                                                     // Read permissions are inherited from the collection.
                                                        db.collection("ming.collections", function (err, collectionsCollection) {
                                                            if (err !== null) {
                                                                callback(err);
                                                            } else {
                                                                collectionsCollection.findOne({
                                                                    name: collectionName
                                                                }, {
                                                                    "_permissions.read": 1
                                                                }, function (err, collectionMetadata) {
                                                                    if (err !== null) {
                                                                        callback(err);
                                                                    } else {
                                                                        if (collectionMetadata === null) {
                                                                            callback(null, null);
                                                                        } else {
                                                                            if (hasPermission("read", collectionMetadata, user) === false) {
                                                                                callback({
                                                                                    name: "Forbidden",
                                                                                    statusCode: 403,
                                                                                    message: "You are not allowed to access this resource"
                                                                                });
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
                                                            }
                                                        });
                                                    } else {
                                                     // Read permissions are explicitly defined.
                                                        if (hasPermission("read", metadata, user) === false) {
                                                            callback({
                                                                name: "Forbidden",
                                                                statusCode: 403,
                                                                message: "You are not allowed to access this resource"
                                                            });
                                                        } else {
                                                            if (document.hasOwnProperty(fieldName) === false) {
                                                                callback(null, null);
                                                            } else {
                                                                callback(null, document[fieldName]);
                                                            }
                                                        }
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
            },
            query: function (collectionName, query, options, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback({
                        name: "Forbidden",
                        statusCode: 403,
                        message: "You are not allowed to access this resource"
                    });
                } else {
                    if (typeof options === "function") {
                        callback = options;
                        options = {};
                    }
                    dataSource.then(function (db) {
                        db.collection("ming.collections", function (err, collectionsCollection) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                collectionsCollection.findOne({
                                    name: collectionName
                                }, function (err, collectionMetadata) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        if (collectionMetadata === null) {
                                            callback(null, null);
                                        } else {
                                            if (hasPermission("read", collectionMetadata, user) === false) {
                                                callback({
                                                    name: "Forbidden",
                                                    statusCode: 403,
                                                    message: "You are not allowed to access this resource"
                                                });
                                            } else {
                                                db.collection(collectionName, function (err, collection) {
                                                    if (err !== null) {
                                                        callback(err);
                                                    } else {
                                                        collection.find(query, options).toArray(function (err, documents) {
                                                            if (err !== null) {
                                                                callback(err);
                                                            } else {
                                                                callback(null, documents.filter(function (document) {
                                                                    var metadata;
                                                                 // Permissions of files are stored in the metadata property.
                                                                    if (collectionName.lastIndexOf(".files") !== -1 && collectionName.lastIndexOf(".files") === collectionName.length - 6) {
                                                                        metadata = document.metadata;
                                                                    } else {
                                                                        metadata = document;
                                                                    }
                                                                 // Only include resources the user is allowed to see.
                                                                    if (metadata._permissions.read === "inherit") {
                                                                     // If permissions are inherited, the user is already allowed to see.
                                                                        return true;
                                                                    } else if (hasPermission("read", metadata, user) === false) {
                                                                        return false;
                                                                    } else {
                                                                        return true;
                                                                    }
                                                                }));
                                                            }
                                                        });
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
            },
            insertCollection: function (collection, user, callback) {
                var collectionName;
                if (collection.hasOwnProperty("name") === false) {
                    callback({
                        name: "Bad Request",
                        statusCode: 400,
                        message: "Missing name."
                    });
                } else {
                    collectionName = collection.name;
                    if (isSystemCollection(collectionName) === true) {
                        callback({
                            name: "Forbidden",
                            statusCode: 403,
                            message: "You are not allowed to access this resource"
                        });
                    } else {
                        dataSource.then(function (db) {
                         // Create collection.
                            db.createCollection(collectionName, {
                                strict: true
                            }, function (err, collection) {
                                if (err !== null) {
                                    callback({
                                        name: "Conflict",
                                        statusCode: 409,
                                        message: "Collection already exists"
                                    });
                                } else {
                                 // Create collection metadata.
                                    db.collection("ming.collections", function (err, collectionsCollection) {
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            collectionsCollection.insert({
                                                name: collectionName,
                                                _permissions: {
                                                    read: [getUserId(user)],
                                                    write: [getUserId(user)],
                                                    add: [getUserId(user)]
                                                }
                                            }, function (err, collections) {
                                                if (err !== null) {
                                                    callback(err);
                                                } else {
                                                    callback(null, collections[0]._id.toHexString());
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        });
                    }
                }
            },
            insertFile: function (prefixName, contentType, file, user, callback) {
                var collectionName;
                collectionName = prefixName + ".files";
                dataSource.then(function (db) {
                 // Get collection metadata, or create it in case it doesn't already exist.
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
                                        read: [getUserId(user)],
                                        write: [getUserId(user)]
                                    }
                                }
                            }, {
                                upsert: true,
                                new: true
                            }, function (err, collectionMetadata) {
                                var grid;
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    if (hasPermission("write", collectionMetadata, user) === false) {
                                        callback({
                                            name: "Forbidden",
                                            statusCode: 403,
                                            message: "You are not allowed to access this resource"
                                        });
                                    } else {
                                        grid = new mongo.Grid(db, prefixName);
                                        grid.put(file, {
                                            content_type: contentType,
                                            metadata: {
                                                _permissions: {
                                                    read: [getUserId(user)],
                                                    write: [getUserId(user)]
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
                });
            },
            insertDocument: function (collectionName, document, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback({
                        name: "Forbidden",
                        statusCode: 403,
                        message: "You are not allowed to access this resource"
                    });
                } else {
                    dataSource.then(function (db) {
                     // Get collection metadata, or create it in case it doesn't already exist.
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
                                        if (hasPermission("add", collectionMetadata, user) === false) {
                                            callback({
                                                name: "Forbidden",
                                                statusCode: 403,
                                                message: "You are not allowed to access this resource"
                                            });
                                        } else {
                                            db.collection(collectionName, function (err, collection) {
                                                if (err !== null) {
                                                    callback(err);
                                                } else {
                                                 // Merge permissions.
                                                    if (document.hasOwnProperty("_permissions") === false) {
                                                        document._permissions = {};
                                                    }
                                                 // Inherit permissions if no permissions are explicitely defined.
                                                    if (document._permissions.hasOwnProperty("read") === false) {
                                                        document._permissions.read = "inherit";
                                                    }
                                                    if (document._permissions.hasOwnProperty("write") === false) {
                                                        document._permissions.write = "inherit";
                                                    }
                                                    collection.insert(document, function (err, documents) {
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
                    });
                }
            },
            updateCollection: function (collectionName, update, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback({
                        name: "Forbidden",
                        statusCode: 403,
                        message: "You are not allowed to access this resource"
                    });
                } else {
                 // Overwrite name of update with collectionName in case someone is cheating.
                    update.name = collectionName;
                    dataSource.then(function (db) {
                        db.collection("ming.collections", function (err, collectionsCollection) {
                            if (err !== null) {
                                callback(err);
                            } else {
                             // Check if user has write access to resource.
                                collectionsCollection.findOne({
                                    name: collectionName
                                }, {
                                    fields: {
                                        "_permissions.write": 1
                                    }
                                }, function (err, document) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                        if (document === null) {
                                            callback(null, false);
                                        } else {
                                            if (hasPermission("write", document, user) === false) {
                                                callback({
                                                    name: "Forbidden",
                                                    statusCode: 403,
                                                    message: "You are not allowed to access this resource"
                                                });
                                            } else {
                                             // Restore _id.
                                                update._id = document._id;
                                                collectionsCollection.save(update, function (err, result) {
                                                    if (err !== null) {
                                                        callback(err);
                                                    } else {
                                                        callback(null, result === 1);
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
            },
            updateDocument: function (collectionName, documentName, update, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback({
                        name: "Forbidden",
                        statusCode: 403,
                        message: "You are not allowed to access this resource"
                    });
                } else {
                    createObjectID(documentName, function (err, id) {
                        if (err !== null) {
                            callback(err);
                        } else {
                         // Overwrite _id of update with id in case someone is cheating.
                            update._id = id;
                            dataSource.then(function (db) {
                                db.collection(collectionName, function (err, collection) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                     // Check if user has write access to resource.
                                        collection.findOne({
                                            _id: id
                                        }, {
                                            "_permissions.write": 1
                                        }, function (err, document) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (document === null) {
                                                    callback(null, false);
                                                } else {
                                                 // Check if the user is allowed to see the resource.
                                                    if (document._permissions.write === "inherit") {
                                                     // Read permissions are inherited from the collection.
                                                        db.collection("ming.collections", function (err, collectionsCollection) {
                                                            if (err !== null) {
                                                                callback(err);
                                                            } else {
                                                                collectionsCollection.findOne({
                                                                    name: collectionName
                                                                }, {
                                                                    "_permissions.write": 1
                                                                }, function (err, collectionMetadata) {
                                                                    if (err !== null) {
                                                                        callback(err);
                                                                    } else {
                                                                        if (collectionMetadata === null) {
                                                                            callback(null, null);
                                                                        } else {
                                                                            if (hasPermission("write", collectionMetadata, user) === false) {
                                                                                callback({
                                                                                    name: "Forbidden",
                                                                                    statusCode: 403,
                                                                                    message: "You are not allowed to access this resource"
                                                                                });
                                                                            } else {
                                                                                collection.save(update, function (err, result) {
                                                                                    if (err !== null) {
                                                                                        callback(err);
                                                                                    } else {
                                                                                        callback(null, result === 1);
                                                                                    }
                                                                                });
                                                                            }
                                                                        }
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    } else {
                                                     // Read permissions are explicitly defined.
                                                        if (hasPermission("write", document, user) === false) {
                                                            callback({
                                                                name: "Forbidden",
                                                                statusCode: 403,
                                                                message: "You are not allowed to access this resource"
                                                            });
                                                        } else {
                                                            collection.save(update, function (err, result) {
                                                                if (err !== null) {
                                                                    callback(err);
                                                                } else {
                                                                    callback(null, result === 1);
                                                                }
                                                            });
                                                        }
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
            },
            deleteFile: function (prefixName, fileName, user, callback) {
                createObjectID(fileName, function (err, id) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        dataSource.then(function (db) {
                            db.collection(prefixName + ".files", function (err, collection) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                 // Get metadata first.
                                    collection.findOne({
                                        _id: id
                                    }, function (err, document) {
                                        var grid;
                                        if (err !== null) {
                                            callback(err);
                                        } else {
                                            if (document === null) {
                                                callback(null, false);
                                            } else {
                                                if (hasPermission("write", document.metadata, user) === false) {
                                                    callback({
                                                        name: "Forbidden",
                                                        statusCode: 403,
                                                        message: "You are not allowed to access this resource"
                                                    });
                                                } else {
                                                    grid = new mongo.Grid(db, prefixName);
                                                    grid.delete(id, function (err, flag) {
                                                        if (err !== null) {
                                                            callback(err);
                                                        } else {
                                                            callback(null, flag);
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
            },
            deleteDocument: function (collectionName, documentName, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback({
                        name: "Forbidden",
                        statusCode: 403,
                        message: "You are not allowed to access this resource"
                    });
                } else {
                    createObjectID(documentName, function (err, id) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            dataSource.then(function (db) {
                                db.collection(collectionName, function (err, collection) {
                                    if (err !== null) {
                                        callback(err);
                                    } else {
                                     // Check if user has write access to resource.
                                        collection.findOne({
                                            _id: id
                                        }, {
                                            "_permissions.write": 1
                                        }, function (err, document) {
                                            if (err !== null) {
                                                callback(err);
                                            } else {
                                                if (document === null) {
                                                    callback(null, false);
                                                } else {
                                                 // Check if the user is allowed to see the resource.
                                                    if (document._permissions.write === "inherit") {
                                                     // Read permissions are inherited from the collection.
                                                        db.collection("ming.collections", function (err, collectionsCollection) {
                                                            if (err !== null) {
                                                                callback(err);
                                                            } else {
                                                                collectionsCollection.findOne({
                                                                    name: collectionName
                                                                }, {
                                                                    "_permissions.write": 1
                                                                }, function (err, collectionMetadata) {
                                                                    if (err !== null) {
                                                                        callback(err);
                                                                    } else {
                                                                        if (collectionMetadata === null) {
                                                                            callback(null, null);
                                                                        } else {
                                                                            if (hasPermission("write", collectionMetadata, user) === false) {
                                                                                callback({
                                                                                    name: "Forbidden",
                                                                                    statusCode: 403,
                                                                                    message: "You are not allowed to access this resource"
                                                                                });
                                                                            } else {
                                                                                collection.remove({
                                                                                    _id: id
                                                                                }, function (err, num) {
                                                                                    if (err !== null) {
                                                                                        callback(err);
                                                                                    } else {
                                                                                        callback(null, num !== 0);
                                                                                    }
                                                                                });
                                                                            }
                                                                        }
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    } else {
                                                     // Read permissions are explicitly defined.
                                                        if (hasPermission("write", document, user) === false) {
                                                            callback({
                                                                name: "Forbidden",
                                                                statusCode: 403,
                                                                message: "You are not allowed to access this resource"
                                                            });
                                                        } else {
                                                            collection.remove({
                                                                _id: id
                                                            }, function (err, num) {
                                                                if (err !== null) {
                                                                    callback(err);
                                                                } else {
                                                                    callback(null, num !== 0);
                                                                }
                                                            });
                                                        }
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
        };
    };

}());
