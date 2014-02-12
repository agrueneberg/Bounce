var errors, expect;

errors = require("../lib/errors");
expect = require("expect.js");

describe("Bounce", function () {

    var dataSource, bounce;

    beforeEach(function () {
        dataSource = require("../lib/data-source")("mongodb://localhost/bounce-tests");
        bounce = require("../lib/bounce")({
            dataSource: dataSource
        });
    });

    afterEach(function (done) {
        dataSource.then(function (db) {
            db.dropDatabase(function () {
                db.close(done);
            });
        });
    });

    describe("users", function () {

        describe("register", function () {

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

            beforeEach(function (done) {
                bounce.register({
                    username: "ming",
                    password: "ming"
                }, done);
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

    describe("collections", function () {

        var documentId;

        beforeEach(function (done) {
            bounce.register({
                username: "ming",
                password: "ming"
            }, function (err, id) {
                bounce.register({
                    username: "flash",
                    password: "flash"
                }, function (err, id) {
                    bounce.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        bounce.insertDocument("planets", {
                            name: "Mongo"
                        }, user, function (err, id) {
                            documentId = id;
                            done();
                        });
                    });
                });
            });
        });

        describe("getCollections", function () {

            it("should allow user ming to see the collection", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getCollections(user, function (err, collections) {
                        expect(err).to.be(null);
                        expect(collections.length).to.eql(1);
                        expect(collections[0].name).to.eql("planets");
                        done();
                    });
                });
            });

            it("should not allow user flash to see the collection", function (done) {
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

            it("should not allow the public user to see the collection", function (done) {
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
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getCollection("system.users", user, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });
            });

            it("should throw an error if the collection does not exist", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getCollection("lizards", user, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });
            });

            it("should allow user ming to see the collection", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getCollection("planets", user, function (err, collection) {
                        expect(err).to.be(null);
                        expect(collection).to.not.be(null);
                        expect(collection.name).to.be("planets");
                        done();
                    });
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
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.updateCollection("system.users", {}, user, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });
            });

            it("should throw an error if the collection does not exist", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.updateCollection("lizards", {}, user, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });
            });

            it("should allow user ming to update the collection", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getCollection("planets", user, function (err, collection) {
                     // Mark collection.
                        collection.updated = true;
                        bounce.updateCollection("planets", collection, user, function (err) {
                            bounce.getCollection("planets", user, function (err, updatedCollection) {
                                expect(err).to.be(null);
                                expect(updatedCollection.updated).to.be(true);
                                done();
                            });
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

        });

        describe("deleteCollection", function () {

            it("should throw an error if the collection is a system collection", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.deleteCollection("system.users", user, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });
            });

            it("should throw an error if the collection does not exist", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.deleteCollection("lizards", user, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });
            });

            it("should allow user ming to delete the collection", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.deleteCollection("planets", user, function (err) {
                        bounce.getCollection("planets", user, function (err) {
                            expect(err).to.be.a(errors.NotFound);
                            done();
                        });
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

        var documentId;

        beforeEach(function (done) {
            bounce.register({
                username: "ming",
                password: "ming"
            }, function (err, id) {
                bounce.register({
                    username: "flash",
                    password: "flash"
                }, function (err, id) {
                    bounce.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        bounce.insertDocument("planets", {
                            name: "Mongo"
                        }, user, function (err, id) {
                            documentId = id;
                            done();
                        });
                    });
                });
            });
        });

        describe("getDocument", function () {

            it("should throw an error if the document ID is not valid", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getDocument("planets", "123", user, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });
            });

            it("should throw an error if the collection is a system collection", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getDocument("system.users", "123", user, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });
            });

            it("should throw an error if the document does not exist", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getDocument("planets", "52ebdb27b31667132ad4ae6c", user, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });
            });

            it("should allow user ming to see the document", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getDocument("planets", documentId, user, function (err, document) {
                        expect(err).to.be(null);
                        expect(document.name).to.be("Mongo");
                        done();
                    });
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
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getField("planets", "123", "name", user, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });
            });

            it("should throw an error if the collection is a system collection", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getField("system.users", "123", "name", user, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });
            });

            it("should throw an error if the document does not exist", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getField("planets", "52ebdb27b31667132ad4ae6c", "name", user, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });
            });

            it("should throw an error if the field does not exist", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getField("planets", documentId, "population", user, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });
            });

            it("should allow user ming to see the field", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getField("planets", documentId, "name", user, function (err, field) {
                        expect(err).to.be(null);
                        expect(field).to.be("Mongo");
                        done();
                    });
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
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.insertDocument("system.users", {
                        usr: "hacker",
                        pwd: "hacker"
                    }, user, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });
            });

            it("should allow user ming to add a new document to the collection", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.insertDocument("planets", {
                        name: "Earth"
                    }, user, function (err, id) {
                        expect(err).to.be(null);
                        expect(id).to.not.be(null);
                        done();
                    });
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
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.insertDocument("animals", {
                        name: "Lizard"
                    }, user, function (err, id) {
                        expect(err).to.be(null);
                        expect(id).to.not.be(null);
                        bounce.getCollection("animals", user, function (err, collection) {
                            expect(err).to.be(null);
                            expect(collection.name).to.be("animals");
                            done();
                        });
                    });
                });
            });

        });

        describe("updateDocument", function () {

            it("should throw an error if the document ID is not valid", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.updateDocument("planets", "123", {}, user, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });
            });

            it("should throw an error if the collection is a system collection", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.updateDocument("system.users", "123", {}, user, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });
            });

            it("should allow user ming to update the document", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getDocument("planets", documentId, user, function (err, document) {
                        document.name = "Mongo the Great";
                        bounce.updateDocument("planets", documentId, document, user, function (err) {
                            expect(err).to.be(null);
                            bounce.getDocument("planets", documentId, user, function (err, document) {
                                expect(err).to.be(null);
                                expect(document.name).to.be("Mongo the Great");
                                done();
                            });
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

        });

        describe("deleteDocument", function () {

            it("should throw an error if the document ID is not valid", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.deleteDocument("planets", "123", user, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });
            });

            it("should throw an error if the collection is a system collection", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.deleteDocument("system.users", "123", user, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });
            });

            it("should throw an error if the document does not exist", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.deleteDocument("planets", "52ebdb27b31667132ad4ae6c", user, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });
            });

            it("should allow user ming to delete the document", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.deleteDocument("planets", documentId, user, function (err) {
                        expect(err).to.be(null);
                        bounce.getDocument("planets", documentId, user, function (err) {
                            expect(err).to.be.a(errors.NotFound);
                            done();
                        });
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

        var documentId;

        beforeEach(function (done) {
            bounce.register({
                username: "ming",
                password: "ming"
            }, function (err, id) {
                bounce.register({
                    username: "flash",
                    password: "flash"
                }, function (err, id) {
                    bounce.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        bounce.insertDocument("planets", {
                            name: "Mongo"
                        }, user, function (err, id) {
                            documentId = id;
                            done();
                        });
                    });
                });
            });
        });

        describe("getPermissions", function () {

            it("should throw an error if the resource path is not valid", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getPermissions("ming", user, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });
            });

            it("should throw an error if the resource path points to a collection and the collection does not exist", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getPermissions("/lizards", user, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });
            });

            it("should throw an error if the resource path points to a document and the document ID is not valid", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getPermissions("/lizards/thebigone", user, function (err) {
                        expect(err).to.be.a(errors.BadRequest);
                        done();
                    });
                });
            });

            it("should throw an error if the resource path points to a document and the document does not exist", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getPermissions("/lizards/52ebdb27b31667132ad4ae6c", user, function (err) {
                        expect(err).to.be.a(errors.NotFound);
                        done();
                    });
                });
            });

            it("should allow user ming to see that the resource inherits from another resource", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getPermissions("/planets/" + documentId, user, function (err, permissions) {
                        expect(err).to.be(null);
                        expect(permissions._links.inherit.href).to.be("/planets");
                        done();
                    });
                });
            });

            it("should not allow user flash to see the permissions for the resource", function (done) {
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

            it("should not allow the public user to see the permissions for the resource", function (done) {
                bounce.authenticate(null, function (err, user) {
                    bounce.getPermissions("/planets/" + documentId, user, function (err) {
                        expect(err).to.be.a(errors.Forbidden);
                        done();
                    });
                });
            });

        });

        describe("updatePermissions", function () {

            it("should be possible for user ming to allow user flash to see the document, but not to update it", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.updatePermissions("/planets/" + documentId, {
                        read: {
                            ming: "all",
                            flash: "all"
                        }
                    }, user, function (err) {
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
            });

            it("should be possible for user ming to allow user flash to update the document, but not read it", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getDocument("planets", documentId, user, function (err, document) {
                        bounce.updatePermissions("/planets/" + documentId, {
                            write: {
                                ming: "all",
                                flash: "all"
                            }
                        }, user, function (err) {
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
            });

            it("should be possible for user ming to allow user flash both to see and update the document", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.getDocument("planets", documentId, user, function (err, document) {
                        bounce.updatePermissions("/planets/" + documentId, {
                            read: {
                                ming: "all",
                                flash: "all"
                            },
                            write: {
                                ming: "all",
                                flash: "all"
                            }
                        }, user, function (err) {
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
            });

            it("should be possible for user ming to specify that the document inherits permissions from a specific URL", function (done) {
                bounce.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    bounce.updatePermissions("/planets/" + documentId, {
                        _links: {
                            inherit: {
                                href: "https://dl.dropboxusercontent.com/s/gomr9p2wwqyjj8i/public.json"
                            }
                        }
                    }, user, function (err) {
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
