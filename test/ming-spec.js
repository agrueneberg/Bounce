var expect;

expect = require("expect.js");

describe("Ming", function () {

    var dataSource, ming;

    beforeEach(function () {
        dataSource = require("../lib/data-source")("mongodb://localhost/ming-tests");
        ming = require("../lib/ming")({
            dataSource: dataSource
        });
    });

    afterEach(function (done) {
        dataSource.then(function (db) {
            db.dropDatabase(done);
        });
    });

    describe("register", function () {

        it("should throw an error if username or password are not given", function (done) {
            ming.register({}, function (err) {
                expect(err).to.not.be(null);
                ming.register({
                    username: "ming"
                }, function (err) {
                    expect(err).to.not.be(null);
                    ming.register({
                        password: "ming"
                    }, function (err) {
                        expect(err).to.not.be(null);
                        done();
                    });
                });
            });
        });

        it("should not allow the same username twice", function (done) {
            ming.register({
                username: "ming",
                password: "ming"
            }, function (err, id) {
                ming.register({
                    username: "ming",
                    password: "ming"
                }, function (err) {
                    expect(err.statusCode).to.be(409);
                    done();
                });
            });
        });

        it("should register a user", function (done) {
            ming.register({
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
            ming.register({
                username: "ming",
                password: "ming"
            }, done);
        });

        it("should throw an error if username is not known", function (done) {
            ming.authenticate({
                username: "flash",
                password: "flash"
            }, function (err, user) {
                expect(err).to.be(null);
                expect(user).to.be(null);
                done();
            });
        });

        it("should throw an error if password is wrong", function (done) {
            ming.authenticate({
                username: "ming",
                password: "flash"
            }, function (err, user) {
                expect(err).to.be(null);
                expect(user).to.be(null);
                done();
            });
        });

        it("should authenticate a user", function (done) {
            ming.authenticate({
                username: "ming",
                password: "ming"
            }, function (err, user) {
                expect(err).to.be(null);
                expect(user.username).to.be("ming");
                done();
            });
        });

        it("should fall back to a public user if no credentials are provided", function (done) {
            ming.authenticate(null, function (err, user) {
                expect(err).to.be(null);
                expect(user).to.be("public");
                done();
            });
        });

    });

    describe("collections & documents", function () {

        var documentId, userMingId, userFlashId;

        beforeEach(function (done) {
            ming.register({
                username: "ming",
                password: "ming"
            }, function (err, id) {
                userMingId = id;
                ming.register({
                    username: "flash",
                    password: "flash"
                }, function (err, id) {
                    userFlashId = id;
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.insertDocument("planets", {
                            name: "Mongo"
                        }, user, function (err, id) {
                            documentId = id;
                            done();
                        });
                    });
                });
            });
        });

        describe("collections", function () {

            describe("getCollections", function () {

                it("should allow user ming read access to the collection", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getCollections(user, function (err, collections) {
                            expect(err).to.be(null);
                            expect(collections).to.eql(["planets"]);
                            done();
                        });
                    });
                });

                it("should deny user flash read access to the collection", function (done) {
                    ming.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        ming.getCollections(user, function (err, collections) {
                            expect(err).to.be(null);
                            expect(collections).to.eql([]);
                            done();
                        });
                    });
                });

            });

            describe("getCollection", function () {

                it("should throw an error if collection is a system collection", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getCollection("system.users", user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

                it("should return null if collection doesn't exist", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getCollection("lizards", user, function (err, collection) {
                            expect(err).to.be(null);
                            expect(collection).to.be(null);
                            done();
                        });
                    });
                });

                it("should allow user ming read access to the collection", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getCollection("planets", user, function (err, collection) {
                            expect(err).to.be(null);
                            expect(collection).to.not.be(null);
                            expect(collection.name).to.be("planets");
                            done();
                        });
                    });
                });

                it("should deny user flash read access to the collection", function (done) {
                    ming.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        ming.getCollection("planets", user, function (err, collection) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

            });

            describe("updateCollection", function () {

                it("should throw an error if collection is a system collection", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.updateCollection("system.users", {}, user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

                it("should return false if collection doesn't exist", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.updateCollection("lizards", {}, user, function (err, updated) {
                            expect(err).to.be(null);
                            expect(updated).to.be(false);
                            done();
                        });
                    });
                });

                it("should allow user ming write access to the collection", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getCollection("planets", user, function (err, collection) {
                         // Mark collection.
                            collection.updated = true;
                            ming.updateCollection("planets", collection, user, function (err, updated) {
                                ming.getCollection("planets", user, function (err, updatedCollection) {
                                    expect(err).to.be(null);
                                    expect(updatedCollection.updated).to.be(true);
                                    done();
                                });
                            });
                        });
                    });
                });

                it("should deny user flash write access to the collection", function (done) {
                    ming.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        ming.updateCollection("planets", {}, user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

            });

        });

        describe("documents", function () {

            describe("getDocument", function () {

                it("should throw an error if documentName is not a valid ObjectID", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getDocument("planets", "123", user, function (err) {
                            expect(err.statusCode).to.be(400);
                            done();
                        });
                    });
                });

                it("should throw an error if collection is a system collection", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getDocument("system.users", "123", user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

                it("should return null if document doesn't exist", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getDocument("planets", "52ebdb27b31667132ad4ae6c", user, function (err, document) {
                            expect(err).to.be(null);
                            expect(document).to.be(null);
                            done();
                        });
                    });
                });

                it("should allow user ming read access to the document", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getDocument("planets", documentId, user, function (err, document) {
                            expect(err).to.be(null);
                            expect(document.name).to.be("Mongo");
                            done();
                        });
                    });
                });

                it("should deny user flash read access to the document", function (done) {
                    ming.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        ming.getDocument("planets", documentId, user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

            });

            describe("insertDocument", function () {

                it("should throw an error if collection is a system collection", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.insertDocument("system.users", {
                            usr: "hacker",
                            pwd: "hacker"
                        }, user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

                it("should allow user ming write access to the collection", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.insertDocument("planets", {
                            name: "Earth"
                        }, user, function (err, id) {
                            expect(err).to.be(null);
                            expect(id).to.not.be(null);
                            done();
                        });
                    });
                });

                it("should deny user flash write access to the collection", function (done) {
                    ming.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        ming.insertDocument("planets", {
                            name: "Earth"
                        }, user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

                it("should create a new collection if the collection does not exist yet", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.insertDocument("animals", {
                            name: "Lizard"
                        }, user, function (err, id) {
                            expect(err).to.be(null);
                            expect(id).to.not.be(null);
                            ming.getCollection("animals", user, function (err, collection) {
                                expect(err).to.be(null);
                                expect(collection.name).to.be("animals");
                                done();
                            });
                        });
                    });
                });

            });

            describe("updateDocument", function () {

                it("should throw an error if documentName is not a valid ObjectID", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.updateDocument("planets", "123", {}, user, function (err) {
                            expect(err.statusCode).to.be(400);
                            done();
                        });
                    });
                });

                it("should throw an error if collection is a system collection", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.updateDocument("system.users", "123", {}, user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

                it("should allow user ming write access to the document", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getDocument("planets", documentId, user, function (err, document) {
                            document.name = "Mongo the Great";
                            ming.updateDocument("planets", documentId, document, user, function (err, updated) {
                                expect(err).to.be(null);
                                expect(updated).to.be(true);
                                ming.getDocument("planets", documentId, user, function (err, document) {
                                    expect(err).to.be(null);
                                    expect(document.name).to.be("Mongo the Great");
                                    done();
                                });
                            });
                        });
                    });
                });

                it("should deny user flash write access to the document", function (done) {
                    ming.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        ming.updateDocument("planets", documentId, {
                            name: "Mongo, Earth II"
                        }, user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

                it("should be possible for user ming to allow user flash read access but no write access to the document", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getDocument("planets", documentId, user, function (err, document) {
                            document._permissions.read.push(userFlashId);
                            ming.updateDocument("planets", documentId, document, user, function (err) {
                                ming.authenticate({
                                    username: "flash",
                                    password: "flash"
                                }, function (err, user) {
                                    ming.getDocument("planets", documentId, user, function (err, document) {
                                        expect(err).to.be(null);
                                        expect(document.name).to.be("Mongo");
                                        document.name = "Mongo, Earth II";
                                        ming.updateDocument("planets", documentId, document, user, function (err) {
                                            expect(err.statusCode).to.be(403);
                                            done();
                                        });
                                    });
                                });
                            });
                        });
                    });
                });

                it("should be possible for user ming to allow user flash write access but no read access to the document", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getDocument("planets", documentId, user, function (err, document) {
                            document._permissions.write.push(userFlashId);
                            ming.updateDocument("planets", documentId, document, user, function (err) {
                                ming.authenticate({
                                    username: "flash",
                                    password: "flash"
                                }, function (err, user) {
                                    document.name = "Mongo, Earth II";
                                    ming.updateDocument("planets", documentId, document, user, function (err, updated) {
                                        expect(err).to.be(null);
                                        expect(updated).to.be(true);
                                        ming.getDocument("planets", documentId, user, function (err) {
                                            expect(err.statusCode).to.be(403);
                                            done();
                                        });
                                    });
                                });
                            });
                        });
                    });
                });

                it("should be possible for user ming to allow user flash both read and write access to the document", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getDocument("planets", documentId, user, function (err, document) {
                            document._permissions.read.push(userFlashId);
                            document._permissions.write.push(userFlashId);
                            ming.updateDocument("planets", documentId, document, user, function (err) {
                                ming.authenticate({
                                    username: "flash",
                                    password: "flash"
                                }, function (err, user) {
                                    document.name = "Mongo, Earth II";
                                    ming.updateDocument("planets", documentId, document, user, function (err, updated) {
                                        expect(err).to.be(null);
                                        expect(updated).to.be(true);
                                        ming.getDocument("planets", documentId, user, function (err, document) {
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

            });

            describe("deleteDocument", function () {

                it("should throw an error if documentName is not a valid ObjectID", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.deleteDocument("planets", "123", user, function (err) {
                            expect(err.statusCode).to.be(400);
                            done();
                        });
                    });
                });

                it("should throw an error if collection is a system collection", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.deleteDocument("system.users", "123", user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

                it("should return false if document doesn't exist", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.deleteDocument("planets", "52ebdb27b31667132ad4ae6c", user, function (err, deleted) {
                            expect(err).to.be(null);
                            expect(deleted).to.be(false);
                            done();
                        });
                    });
                });

                it("should allow user ming write access to the document", function (done) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.deleteDocument("planets", documentId, user, function (err, deleted) {
                            expect(err).to.be(null);
                            expect(deleted).to.be(true);
                            ming.getDocument("planets", documentId, user, function (err, document) {
                                expect(err).to.be(null);
                                expect(document).to.be(null);
                                done();
                            });
                        });
                    });
                });

                it("should deny user flash write access to the document", function (done) {
                    ming.authenticate({
                        username: "flash",
                        password: "flash"
                    }, function (err, user) {
                        ming.deleteDocument("planets", documentId, user, function (err) {
                            expect(err.statusCode).to.be(403);
                            done();
                        });
                    });
                });

            });

        });

    });

});
