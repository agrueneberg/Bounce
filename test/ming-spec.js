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

    });

    describe("collections", function () {

        beforeEach(function (done) {
            ming.register({
                username: "ming",
                password: "ming"
            }, function () {
                ming.register({
                    username: "flash",
                    password: "flash"
                }, function () {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.insertDocument("planets", {
                            name: "Mongo"
                        }, user, done);
                    });
                });
            });
        });

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

            beforeEach(function (done) {
                ming.register({
                    username: "aura",
                    password: "aura"
                }, function (err, id) {
                    ming.authenticate({
                        username: "ming",
                        password: "ming"
                    }, function (err, user) {
                        ming.getCollection("planets", user, function (err, collection) {
                         // Strip _count.
                            delete collection._count;
                         // Add aura to readers.
                            collection._permissions.read.push(id);
                            ming.updateCollection("planets", collection, user, done);
                        });
                    });
                });
            });

            it("should throw an error if collection doesn't exist", function (done) {
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

            it("should allow user ming read access to the collection and the document", function (done) {
                ming.authenticate({
                    username: "ming",
                    password: "ming"
                }, function (err, user) {
                    ming.getCollection("planets", user, function (err, collection) {
                        expect(err).to.be(null);
                        expect(collection).to.not.be(null);
                        expect(collection._count).to.be(1);
                        done();
                    });
                });
            });

            it("should allow user aura read access to the collection but not the document", function (done) {
                ming.authenticate({
                    username: "aura",
                    password: "aura"
                }, function (err, user) {
                    ming.getCollection("planets", user, function (err, collection) {
                        expect(err).to.be(null);
                        expect(collection).to.not.be(null);
                        expect(collection._count).to.be(0);
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
                        expect(err).to.not.be(null);
                        done();
                    });
                });
            });

        });

    });

});
