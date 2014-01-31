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

});
