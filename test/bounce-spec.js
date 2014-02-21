var errors, expect;

errors = require("../lib/errors");
expect = require("expect.js");

describe("Bounce", function () {

    var dataSource, bounce;

    before(function () {
        dataSource = require("../lib/data-source")("mongodb://localhost/bounce-tests");
        bounce = require("../lib/bounce")({
            dataSource: dataSource
        });
    });

    after(function (done) {
        dataSource.then(function (db) {
            db.close(done);
        });
    });

    describe("users (bcrypt is slow)", function () {

        describe("register", function () {

            afterEach(function (done) {
                dataSource.then(function (db) {
                    db.dropDatabase(done);
                });
            });

            it("should throw an error if username or password are not given", function (done) {
                bounce.register({}, function (err) {
                    expect(err).to.not.be(null);
                    bounce.register({
                        username: "ming"
                    }, function (err) {
                        expect(err).to.not.be(null);
                        bounce.register({
                            password: "ming"
                        }, function (err) {
                            expect(err).to.not.be(null);
                            done();
                        });
                    });
                });
            });

            it("should not allow the same username twice", function (done) {
                bounce.register({
                    username: "ming",
                    password: "ming"
                }, function (err, id) {
                    bounce.register({
                        username: "ming",
                        password: "ming"
                    }, function (err) {
                        expect(err).to.be.a(errors.Conflict);
                        done();
                    });
                });
            });

            it("should not allow the username public", function (done) {
                bounce.register({
                    username: "public",
                    password: "public"
                }, function (err, id) {
                    expect(err).to.be.a(errors.Conflict);
                    done();
                });
            });

            it("should not allow the username authenticated", function (done) {
                bounce.register({
                    username: "authenticated",
                    password: "authenticated"
                }, function (err, id) {
                    expect(err).to.be.a(errors.Conflict);
                    done();
                });
            });

            it("should register a user", function (done) {
                bounce.register({
                    username: "ming",
                    password: "ming"
                }, function (err, id) {
                    expect(err).to.be(null);
                    done();
                });
            });

        });

        describe("authenticate", function () {

            before(function (done) {
                bounce.register({
                    username: "ming",
                    password: "ming"
                }, done);
            });

            after(function (done) {
                dataSource.then(function (db) {
                    db.dropDatabase(done);
                });
            });

            it("should throw an error if username is not known", function (done) {
                bounce.authenticate({
                    username: "flash",
                    password: "flash"
                }, function (err, user) {
                    expect(err).to.be(null);
                    expect(user).to.be(null);
                    done();
                });
            });

            it("should throw an error if password is wrong", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "flash"
                }, function (err, user) {
                    expect(err).to.be(null);
                    expect(user).to.be(null);
                    done();
                });
            });

            it("should authenticate a user", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    expect(err).to.be(null);
                    expect(user.username).to.be("ming");
                    done();
                });
            });

            it("should fall back to a public user if no credentials are provided", function (done) {
                bounce.authenticate(null, function (err, user) {
                    expect(err).to.be(null);
                    expect(user.username).to.be("public");
                    done();
                });
            });

        });

    });

    describe("core", function () {

        var authenticatedUser, documentId;

        before(function (done) {
            bounce.register({
                username: "ming",
                password: "ming"
            }, function () {
                bounce.register({
                    username: "flash",
                    password: "flash"
                }, function () {
                    bounce.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        authenticatedUser = user;
                        done();
                    });
                });
            });
        });

        beforeEach(function (done) {
            bounce.insertDocument("planets", {
                name: "Mongo"
            }, authenticatedUser, function (err, id) {
                documentId = id;
                done();
            });
        });

        afterEach(function (done) {
            bounce.deleteCollection("planets", authenticatedUser, function () {
             // Ignore errors.
                done();
            });
        });

        after(function (done) {
            dataSource.then(function (db) {
                db.dropDatabase(done);
            });
        });

        describe("collections", function () {

            describe("getCollections", function () {

                it("should allow user ming to see the collection in the collection list", function (done) {
                    bounce.getCollections(authenticatedUser, function (err, collections) {
                        expect(err).to.be(null);
                        expect(collections.length).to.eql(1);
                        expect(collections[0].name).to.eql("planets");
                        done();
                    });
                });

                it("should not allow user flash to see the collection in the collection list", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.getCollections(user, function (err, collections) {
                            expect(err).to.be(null);
                            expect(collections).to.eql([]);
                            done();
                        });
                    });
                });

                it("should not allow the public user to see the collection in the collection list", function (done) {
                    bounce.authenticate(null, function (err, user) {
                        bounce.getCollections(user, function (err, collections) {
                            expect(err).to.be(null);
                            expect(collections).to.eql([]);
                            done();
                        });
                    });
                });

            });

            describe("getCollection", function () {

                it("should throw an error if the collection is a system collection", function (done) {
                    bounce.getCollection("system.users", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });

                it("should throw an error if the collection does not exist", function (done) {
                    bounce.getCollection("animals", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });

                it("should allow user ming to see the collection", function (done) {
                    bounce.getCollection("planets", authenticatedUser, function (err, collection) {
                        expect(err).to.be(null);
                        expect(collection).to.not.be(null);
                        expect(collection.name).to.be("planets");
                        done();
                    });
                });

                it("should not allow user flash to see the collection", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.getCollection("planets", user, function (err, collection) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should not allow the public to see the collection", function (done) {
                    bounce.authenticate(null, function (err, user) {
                        bounce.getCollection("planets", user, function (err, collection) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

            });

            describe("updateCollection", function () {

                it("should throw an error if the collection is a system collection", function (done) {
                    bounce.updateCollection("system.users", {}, authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });

                it("should throw an error if the collection does not exist", function (done) {
                    bounce.updateCollection("animals", {}, authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });

                it("should allow user ming to update the collection", function (done) {
                    bounce.getCollection("planets", authenticatedUser, function (err, collection) {
                     // Mark collection.
                        collection.updated = true;
                        bounce.updateCollection("planets", collection, authenticatedUser, function (err) {
                            bounce.getCollection("planets", authenticatedUser, function (err, updatedCollection) {
                                expect(err).to.be(null);
                                expect(updatedCollection.updated).to.be(true);
                                done();
                            });
                        });
                    });
                });

                it("should not allow user flash to update the collection", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.updateCollection("planets", {}, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should not allow the public user to update the collection", function (done) {
                    bounce.authenticate(null, function (err, user) {
                        bounce.updateCollection("planets", {}, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should sanitize the collection to update", function (done) {
                    bounce.getCollection("planets", authenticatedUser, function (err, collection) {
                        collection.name = "moons";
                        collection._links = {
                            self: {
                                href: "/"
                            },
                            governance: {
                                href: "/"
                            },
                            test: {
                                href: "/"
                            }
                        };
                        collection._embedded = {};
                        collection._creator = "flash";
                        collection._permissions = {};
                        bounce.updateCollection("planets", collection, authenticatedUser, function (err) {
                            bounce.getCollection("planets", authenticatedUser, function (err, updatedCollection) {
                                expect(err).to.be(null);
                                expect(updatedCollection.name).to.be("planets");
                                expect(updatedCollection._links).to.eql({
                                    test: {
                                        href: "/"
                                    }
                                });
                                expect(updatedCollection._embedded).to.be(undefined);
                                expect(updatedCollection._creator).to.be(undefined);
                                expect(updatedCollection._permissions).to.be(undefined);
                                done();
                            });
                        });
                    });
                });

            });

            describe("deleteCollection", function () {

                it("should throw an error if the collection is a system collection", function (done) {
                    bounce.deleteCollection("system.users", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });

                it("should throw an error if the collection does not exist", function (done) {
                    bounce.deleteCollection("animals", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });

                it("should allow user ming to delete the collection", function (done) {
                    bounce.deleteCollection("planets", authenticatedUser, function (err) {
                        bounce.getCollection("planets", authenticatedUser, function (err) {
                            expect(err).to.be.a(errors.NotFound);
                            done();
                        });
                    });
                });

                it("should not allow user flash to delete the collection", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.deleteCollection("planets", user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should not allow the public user to update the collection", function (done) {
                    bounce.authenticate(null, function (err, user) {
                        bounce.deleteCollection("planets", user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

            });

        });

        describe("documents", function () {

            describe("getDocument", function () {

                it("should throw an error if the document ID is not valid", function (done) {
                    bounce.getDocument("planets", "123", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });

                it("should throw an error if the collection is a system collection", function (done) {
                    bounce.getDocument("system.users", "123", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });

                it("should throw an error if the document does not exist", function (done) {
                    bounce.getDocument("planets", "52ebdb27b31667132ad4ae6c", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });

                it("should allow user ming to see the document", function (done) {
                    bounce.getDocument("planets", documentId, authenticatedUser, function (err, document) {
                        expect(err).to.be(null);
                        expect(document.name).to.be("Mongo");
                        done();
                    });
                });

                it("should not allow user flash to see the document", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.getDocument("planets", documentId, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should not allow the public user to see the document", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.getDocument("planets", documentId, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

            });

            describe("getField", function () {

                it("should throw an error if the document ID is not valid", function (done) {
                    bounce.getField("planets", "123", "name", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });

                it("should throw an error if the collection is a system collection", function (done) {
                    bounce.getField("system.users", "123", "name", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });

                it("should throw an error if the document does not exist", function (done) {
                    bounce.getField("planets", "52ebdb27b31667132ad4ae6c", "name", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });

                it("should throw an error if the field does not exist", function (done) {
                    bounce.getField("planets", documentId, "population", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });

                it("should allow user ming to see the field", function (done) {
                    bounce.getField("planets", documentId, "name", authenticatedUser, function (err, field) {
                        expect(err).to.be(null);
                        expect(field).to.be("Mongo");
                        done();
                    });
                });

                it("should not allow user flash to see the field", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.getField("planets", documentId, "name", user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should not allow user flash to see the field", function (done) {
                    bounce.authenticate(null, function (err, user) {
                        bounce.getField("planets", documentId, "name", user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

            });

            describe("insertDocument", function () {

                it("should throw an error if the collection is a system collection", function (done) {
                    bounce.insertDocument("system.users", {
                        usr: "hacker",
                        pwd: "hacker"
                    }, authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });

                it("should allow user ming to add a new document to the collection", function (done) {
                    bounce.insertDocument("planets", {
                        name: "Earth"
                    }, authenticatedUser, function (err, id) {
                        expect(err).to.be(null);
                        expect(id).to.not.be(null);
                        done();
                    });
                });

                it("should not allow user flash to add a new document to the collection", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.insertDocument("planets", {
                            name: "Earth"
                        }, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should create a new collection if the collection does not exist", function (done) {
                    bounce.insertDocument("animals", {
                        name: "Lizard"
                    }, authenticatedUser, function (err, id) {
                        expect(err).to.be(null);
                        expect(id).to.not.be(null);
                        bounce.getCollection("animals", authenticatedUser, function (err, collection) {
                            expect(err).to.be(null);
                            expect(collection.name).to.be("animals");
                            bounce.deleteCollection("animals", authenticatedUser, done);
                        });
                    });
                });

                it("should sanitize the document to insert", function (done) {
                    bounce.insertDocument("planets", {
                        _links: {
                            self: {
                                href: "/"
                            },
                            governance: {
                                href: "/"
                            },
                            test: {
                                href: "/"
                            }
                        },
                        _embedded: {},
                        _creator: "flash",
                        _permissions: {}
                    }, authenticatedUser, function (err, id) {
                        bounce.getDocument("planets", id, authenticatedUser, function (err, document) {
                            expect(err).to.be(null);
                            expect(document._links).to.eql({
                                test: {
                                    href: "/"
                                }
                            });
                            expect(document._embedded).to.be(undefined);
                            expect(document._creator).to.be(undefined);
                            expect(document._permissions).to.be(undefined);
                            done();
                        });
                    });
                });

            });

            describe("updateDocument", function () {

                it("should throw an error if the document ID is not valid", function (done) {
                    bounce.updateDocument("planets", "123", {}, authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });

                it("should throw an error if the collection is a system collection", function (done) {
                    bounce.updateDocument("system.users", "123", {}, authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });

                it("should allow user ming to update the document", function (done) {
                    bounce.getDocument("planets", documentId, authenticatedUser, function (err, document) {
                        document.name = "Mongo the Great";
                        bounce.updateDocument("planets", documentId, document, authenticatedUser, function (err) {
                            expect(err).to.be(null);
                            bounce.getDocument("planets", documentId, authenticatedUser, function (err, document) {
                                expect(err).to.be(null);
                                expect(document.name).to.be("Mongo the Great");
                                done();
                            });
                        });
                    });
                });

                it("should not allow user flash to update the document", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.updateDocument("planets", documentId, {
                            name: "Mongo, Earth II"
                        }, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should not allow the public user to update the document", function (done) {
                    bounce.authenticate(null, function (err, user) {
                        bounce.updateDocument("planets", documentId, {
                            name: "Mongo, Earth II"
                        }, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should sanitize the document to update", function (done) {
                    bounce.getDocument("planets", documentId, authenticatedUser, function (err, document) {
                        document._links = {
                            self: {
                                href: "/"
                            },
                            governance: {
                                href: "/"
                            },
                            test: {
                                href: "/"
                            }
                        };
                        document._embedded = {};
                        document._creator = "flash";
                        document._permissions = {};
                        bounce.updateDocument("planets", documentId, document, authenticatedUser, function (err, id) {
                            bounce.getDocument("planets", documentId, authenticatedUser, function (err, updatedDocument) {
                                expect(err).to.be(null);
                                expect(updatedDocument._links).to.eql({
                                    test: {
                                        href: "/"
                                    }
                                });
                                expect(updatedDocument._embedded).to.be(undefined);
                                expect(updatedDocument._creator).to.be(undefined);
                                expect(updatedDocument._permissions).to.be(undefined);
                                done();
                            });
                        });
                    });
                });

            });

            describe("deleteDocument", function () {

                it("should throw an error if the document ID is not valid", function (done) {
                    bounce.deleteDocument("planets", "123", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });

                it("should throw an error if the collection is a system collection", function (done) {
                    bounce.deleteDocument("system.users", "123", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });

                it("should throw an error if the document does not exist", function (done) {
                    bounce.deleteDocument("planets", "52ebdb27b31667132ad4ae6c", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });

                it("should allow user ming to delete the document", function (done) {
                    bounce.deleteDocument("planets", documentId, authenticatedUser, function (err) {
                        expect(err).to.be(null);
                        bounce.getDocument("planets", documentId, authenticatedUser, function (err) {
                            expect(err).to.be.a(errors.NotFound);
                            done();
                        });
                    });
                });

                it("should not allow user flash to delete the document", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.deleteDocument("planets", documentId, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should not allow the public user to delete the document", function (done) {
                    bounce.authenticate(null, function (err, user) {
                        bounce.deleteDocument("planets", documentId, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

            });

        });

        describe("permissions", function () {
            describe("getPermissions", function () {

                it("should throw an error if the resource path is not valid", function (done) {
                    bounce.getPermissions("ming", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });

                it("should throw an error if the resource path points to a collection and the collection does not exist", function (done) {
                    bounce.getPermissions("/animals", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });

                it("should throw an error if the resource path points to a document and the document ID is not valid", function (done) {
                    bounce.getPermissions("/animals/lizard", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });

                it("should throw an error if the resource path points to a document and the document does not exist", function (done) {
                    bounce.getPermissions("/animals/52ebdb27b31667132ad4ae6c", authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });

                it("should allow user ming to see that the resource inherits from another resource", function (done) {
                    bounce.getPermissions("/planets/" + documentId, authenticatedUser, function (err, permissions) {
                        expect(err).to.be(null);
                        expect(permissions._inherit).to.be("/planets");
                        done();
                    });
                });

                it("should not allow user flash to see the permissions of the resource", function (done) {
                    bounce.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        bounce.getPermissions("/planets/" + documentId, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should not allow the public user to see the permissions of the resource", function (done) {
                    bounce.authenticate(null, function (err, user) {
                        bounce.getPermissions("/planets/" + documentId, user, function (err) {
                            expect(err).to.be.a(errors.Forbidden);
                            done();
                        });
                    });
                });

                it("should allow the system to see the permissions of the collection", function (done) {
                    bounce.getPermissions("/planets", null, function (err, permissions) {
                        expect(err).to.be(null);
                        expect(permissions).to.eql({
                            _inherit: "/"
                        });
                        done();
                    });
                });

                it("should allow the system to see the permissions of the document", function (done) {
                    bounce.getPermissions("/planets/" + documentId, null, function (err, permissions) {
                        expect(err).to.be(null);
                        expect(permissions).to.eql({
                            _inherit: "/planets"
                        });
                        done();
                    });
                });

            });

            describe("updatePermissions", function () {

                it("should throw an error if the permissions are not well-defined", function (done) {
                    bounce.updatePermissions("/planets/" + documentId, {}, authenticatedUser, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        bounce.updatePermissions("/planets/" + documentId, {
                            rules: [{
                                operator: "govern",
                                username: "ming",
                                state: "all"
                            }]
                        }, authenticatedUser, function (err) {
                            expect(err).to.be.a(errors.BadRequest);
                            bounce.updatePermissions("/planets/" + documentId, {
                                rules: [{
                                    operator: "govern",
                                    username: "ming",
                                    state: "all"
                                }, {
                                    operator: "read",
                                    username: "ming",
                                    state: "all"
                                }]
                            }, authenticatedUser, function (err) {
                                expect(err).to.be.a(errors.BadRequest);
                                bounce.updatePermissions("/planets/" + documentId, {
                                    rules: [{
                                        operator: "govern",
                                        username: "ming",
                                        state: "all"
                                    }, {
                                        operator: "read",
                                        username: "ming",
                                        state: "all"
                                    }, {
                                        operator: "write",
                                        username: "ming",
                                        state: "all"
                                    }]
                                }, authenticatedUser, function (err) {
                                    expect(err).to.be.a(errors.BadRequest);
                                    bounce.updatePermissions("/planets/" + documentId, {
                                        _links: {
                                            example: {
                                                href: "http://example.com"
                                            }
                                        }
                                    }, authenticatedUser, function (err) {
                                        expect(err).to.be.a(errors.BadRequest);
                                        bounce.updatePermissions("/planets/" + documentId, {
                                            hello: "world"
                                        }, authenticatedUser, function (err) {
                                            expect(err).to.be.a(errors.BadRequest);
                                            bounce.updatePermissions("/planets/" + documentId, {
                                                rules: [{
                                                    hello: "world"
                                                }]
                                            }, authenticatedUser, function (err) {
                                                expect(err).to.be.a(errors.BadRequest);
                                                done();
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    });
                });

                it("should accept permissions that contain all operators", function (done) {
                    bounce.updatePermissions("/planets/" + documentId, {
                        rules: [{
                            operator: "govern",
                            username: "ming",
                            state: "all"
                        }, {
                            operator: "read",
                            username: "ming",
                            state: "all"
                        }, {
                            operator: "write",
                            username: "ming",
                            state: "all"
                        }, {
                            operator: "add",
                            username: "ming",
                            state: "all"
                        }]
                    }, authenticatedUser, function (err) {
                        expect(err).to.be(null);
                        done();
                    });
                });

                it("should accept permissions that contain only a link", function (done) {
                    bounce.updatePermissions("/planets/" + documentId, {
                        _inherit: "/planets"
                    }, authenticatedUser, function (err) {
                        expect(err).to.be(null);
                        done();
                    });
                });

                it("should accept permissions that contain both operators and a link", function (done) {
                    bounce.updatePermissions("/planets/" + documentId, {
                        _inherit: "/planets",
                        rules: [{
                            operator: "read",
                            username: "ming",
                            state: "all"
                        }]
                    }, authenticatedUser, function (err) {
                        expect(err).to.be(null);
                        done();
                    });
                });

                it("should be possible for user ming to allow user flash to see the document, but not to update it", function (done) {
                    bounce.updatePermissions("/planets/" + documentId, {
                        _inherit: "/planets",
                        rules: [{
                            operator: "read",
                            username: "ming",
                            state: "all"
                        }, {
                            operator: "read",
                            username: "flash",
                            state: "all"
                        }]
                    }, authenticatedUser, function (err) {
                        bounce.authenticate({
                            username: "flash",
                            password: "flash"
                        }, function (err, user) {
                            bounce.getDocument("planets", documentId, user, function (err, document) {
                                expect(err).to.be(null);
                                expect(document.name).to.be("Mongo");
                                document.name = "Mongo, Earth II";
                                bounce.updateDocument("planets", documentId, document, user, function (err) {
                                    expect(err).to.be.a(errors.Forbidden);
                                    done();
                                });
                            });
                        });
                    });
                });

                it("should be possible for user ming to allow user flash to update the document, but not read it", function (done) {
                    bounce.getDocument("planets", documentId, authenticatedUser, function (err, document) {
                        bounce.updatePermissions("/planets/" + documentId, {
                            _inherit: "/planets",
                            rules: [{
                                operator: "write",
                                username: "ming",
                                state: "all"
                            }, {
                                operator: "write",
                                username: "flash",
                                state: "all"
                            }]
                        }, authenticatedUser, function (err) {
                            bounce.authenticate({
                                username: "flash",
                                password: "flash"
                            }, function (err, user) {
                                document.name = "Mongo, Earth II";
                                bounce.updateDocument("planets", documentId, document, user, function (err) {
                                    expect(err).to.be(null);
                                    bounce.getDocument("planets", documentId, user, function (err) {
                                        expect(err).to.be.a(errors.Forbidden);
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });

                it("should be possible for user ming to allow user flash both to see and update the document", function (done) {
                    bounce.getDocument("planets", documentId, authenticatedUser, function (err, document) {
                        bounce.updatePermissions("/planets/" + documentId, {
                            _inherit: "/planets",
                            rules: [{
                                operator: "read",
                                username: "ming",
                                state: "all"
                            }, {
                                operator: "read",
                                username: "flash",
                                state: "all"
                            }, {
                                operator: "write",
                                username: "ming",
                                state: "all"
                            }, {
                                operator: "write",
                                username: "flash",
                                state: "all"
                            }]
                        }, authenticatedUser, function (err) {
                            bounce.authenticate({
                                username: "flash",
                                password: "flash"
                            }, function (err, user) {
                                document.name = "Mongo, Earth II";
                                bounce.updateDocument("planets", documentId, document, user, function (err) {
                                    expect(err).to.be(null);
                                    bounce.getDocument("planets", documentId, user, function (err, document) {
                                        expect(err).to.be(null);
                                        expect(document.name).to.be("Mongo, Earth II");
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });

                it("should be possible for user ming to specify that the document inherits permissions from a specific URL", function (done) {
                    bounce.updatePermissions("/planets/" + documentId, {
                        _inherit: "https://raw2.github.com/agrueneberg/Bounce/master/test/assets/bounce-public.json"
                    }, authenticatedUser, function (err) {
                        bounce.authenticate(null, function (err, user) {
                            bounce.getDocument("planets", documentId, user, function (err, document) {
                                expect(err).to.be(null);
                                expect(document.name).to.be("Mongo");
                                document.name = "Mongo, Earth II";
                                bounce.updateDocument("planets", documentId, document, user, function (err) {
                                    expect(err).to.be.a(errors.Forbidden);
                                    done();
                                });
                            });
                        });
                    });
                });

            });

        });

    });

});
