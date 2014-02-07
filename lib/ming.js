(function () {
    "use strict";

    var errors, mongo, bcrypt, isSystemCollection, createObjectID, inheritsPermission, hasPermission, getUserId;

    errors = require("./errors");
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

    createObjectID = function (hex, callback) {
        var id;
        try {
            id = new mongo.ObjectID(hex);
            callback(null, id);
        } catch (err) {
            callback(new errors.BadRequest(err.message));
        }
    };

    inheritsPermission = function (permission, document) {
        return document._permissions[permission] === "inherit";
    };

    hasPermission = function (permission, document, user) {
        return document._permissions[permission].indexOf(getUserId(user)) !== -1;
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
            getCollections: function (user, callback) {
                dataSource.then(function (db) {
                    db.collection("ming.collections", function (err, collection) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            collection.find().toArray(function (err, collections) {
                                if (err !== null) {
                                    callback(err);
                                } else {
                                    collections = collections.filter(function (collection) {
                                        return hasPermission("read", collection, user);
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
                    callback(new errors.Forbidden());
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
                                            callback(new errors.NotFound());
                                        } else {
                                            if (hasPermission("read", collectionMetadata, user) === false) {
                                                callback(new errors.Forbidden());
                                            } else {
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
            insertCollection: function (body, user, callback) {
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
                            }, function (err, collection) {
                                if (err !== null) {
                                    callback(new errors.Conflict("Collection already exists."));
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
            updateCollection: function (collectionName, body, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback(new errors.Forbidden());
                } else {
                 // Overwrite name in case someone is cheating.
                    body.name = collectionName;
                    dataSource.then(function (db) {
                     // Get collection metadata.
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
                                            callback(new errors.NotFound());
                                        } else {
                                            if (hasPermission("write", collectionMetadata, user) === false) {
                                                callback(new errors.Forbidden());
                                            } else {
                                             // Restore _id.
                                                body._id = collectionMetadata._id;
                                                collectionsCollection.save(body, function (err) {
                                                    if (err !== null) {
                                                        callback(err);
                                                    } else {
                                                        callback(null);
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
            getDocument: function (collectionName, documentName, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback(new errors.Forbidden());
                } else {
                    createObjectID(documentName, function (err, id) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            dataSource.then(function (db) {
                             // Get collection metadata.
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
                                                    callback(new errors.NotFound());
                                                } else {
                                                 // Get document metadata.
                                                    db.collection(collectionName, function (err, collection) {
                                                        if (err !== null) {
                                                            callback(err);
                                                        } else {
                                                            collection.findOne({
                                                                _id: id
                                                            }, function (err, document) {
                                                                var documentMetadata, permission;
                                                                if (err !== null) {
                                                                    callback(err);
                                                                } else {
                                                                    if (document === null) {
                                                                        callback(new errors.NotFound());
                                                                    } else {
                                                                     // Permissions of files are stored in the metadata property.
                                                                        if (collectionName.lastIndexOf(".files") !== -1 && collectionName.lastIndexOf(".files") === collectionName.length - 6) {
                                                                            documentMetadata = document.metadata;
                                                                        } else {
                                                                            documentMetadata = document;
                                                                        }
                                                                        if (inheritsPermission("read", documentMetadata) === true) {
                                                                            documentMetadata = collectionMetadata;
                                                                        }
                                                                        if (hasPermission("read", documentMetadata, user) === false) {
                                                                            callback(new errors.Forbidden());
                                                                        } else {
                                                                            callback(null, document);
                                                                        }
                                                                    }
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
                    });
                }
            },
            getField: function (collectionName, documentName, fieldName, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback(new errors.Forbidden());
                } else {
                    createObjectID(documentName, function (err, id) {
                        if (err !== null) {
                            callback(err);
                        } else {
                            dataSource.then(function (db) {
                             // Get collection metadata.
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
                                                    callback(new errors.NotFound());
                                                } else {
                                                 // Get document metadata.
                                                    db.collection(collectionName, function (err, collection) {
                                                        var fields;
                                                        if (err !== null) {
                                                            callback(err);
                                                        } else {
                                                         // Include metadata.
                                                            fields = {
                                                                "_permissions": 1
                                                            };
                                                            fields[fieldName] = 1;
                                                            collection.findOne({
                                                                _id: id
                                                            }, {
                                                                fields: fields
                                                            }, function (err, document) {
                                                                var documentMetadata, permission;
                                                                if (err !== null) {
                                                                    callback(err);
                                                                } else {
                                                                    if (document === null) {
                                                                        callback(new errors.NotFound());
                                                                    } else {
                                                                     // Permissions of files are stored in the metadata property.
                                                                        if (collectionName.lastIndexOf(".files") !== -1 && collectionName.lastIndexOf(".files") === collectionName.length - 6) {
                                                                            documentMetadata = document.metadata;
                                                                        } else {
                                                                            documentMetadata = document;
                                                                        }
                                                                        if (inheritsPermission("read", documentMetadata) === true) {
                                                                            documentMetadata = collectionMetadata;
                                                                        }
                                                                        if (hasPermission("read", documentMetadata, user) === false) {
                                                                            callback(new errors.Forbidden());
                                                                        } else {
                                                                            if (document.hasOwnProperty(fieldName) === false) {
                                                                                callback(new errors.NotFound());
                                                                            } else {
                                                                                callback(null, document[fieldName]);
                                                                            }
                                                                        }
                                                                    }
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
                    });
                }
            },
            insertDocument: function (collectionName, document, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback(new errors.Forbidden());
                } else {
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
                                            callback(new errors.Forbidden());
                                        } else {
                                            db.collection(collectionName, function (err, collection) {
                                                if (err !== null) {
                                                    callback(err);
                                                } else {
                                                 // Merge permissions.
                                                    if (document.hasOwnProperty("_permissions") === false) {
                                                        document._permissions = {};
                                                    }
                                                 // Inherit permissions if permissions are not explicitly defined.
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
            updateDocument: function (collectionName, documentName, update, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback(new errors.Forbidden());
                } else {
                    createObjectID(documentName, function (err, id) {
                        if (err !== null) {
                            callback(err);
                        } else {
                         // Overwrite _id in case someone is cheating.
                            update._id = id;
                            dataSource.then(function (db) {
                             // Get collection metadata.
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
                                                    callback(new errors.NotFound());
                                                } else {
                                                 // Get document metadata.
                                                    db.collection(collectionName, function (err, collection) {
                                                        if (err !== null) {
                                                            callback(err);
                                                        } else {
                                                            collection.findOne({
                                                                _id: id
                                                            }, function (err, document) {
                                                                var documentMetadata, permission;
                                                                if (err !== null) {
                                                                    callback(err);
                                                                } else {
                                                                    if (document === null) {
                                                                        callback(new errors.NotFound());
                                                                    } else {
                                                                        if (inheritsPermission("write", document) === true) {
                                                                            documentMetadata = collectionMetadata;
                                                                        } else {
                                                                            documentMetadata = document;
                                                                        }
                                                                        if (hasPermission("write", documentMetadata, user) === false) {
                                                                            callback(new errors.Forbidden());
                                                                        } else {
                                                                            collection.save(update, function (err) {
                                                                                if (err !== null) {
                                                                                    callback(err);
                                                                                } else {
                                                                                    callback(null);
                                                                                }
                                                                            });
                                                                        }
                                                                    }
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
                    });
                }
            },
            deleteDocument: function (collectionName, documentName, user, callback) {
                if (isSystemCollection(collectionName) === true) {
                    callback(new errors.Forbidden());
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
                                                    callback(new errors.NotFound());
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
                                                                            callback(new errors.NotFound());
                                                                        } else {
                                                                            if (hasPermission("write", collectionMetadata, user) === false) {
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
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    } else {
                                                     // Read permissions are explicitly defined.
                                                        if (hasPermission("write", document, user) === false) {
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
                    callback(new errors.Forbidden());
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
                                            callback(new errors.NotFound());
                                        } else {
                                            if (hasPermission("read", collectionMetadata, user) === false) {
                                                callback(new errors.Forbidden());
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
                                                callback(new errors.NotFound());
                                            } else {
                                                if (hasPermission("read", document.metadata, user) === false) {
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
                                        callback(new errors.Forbidden());
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
                                                callback(new errors.NotFound());
                                            } else {
                                                if (hasPermission("write", document.metadata, user) === false) {
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
                                        }
                                    });
                                }
                            });
                        });
                    }
                });
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
            },
            register: function (user, callback) {
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
            }
        };
    };

}());
