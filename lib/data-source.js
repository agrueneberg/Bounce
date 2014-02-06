(function () {
    "use strict";

    var Promise, mongo;

    Promise = require("bluebird");
    mongo = require("mongodb");

    module.exports = function (connectionString) {
        return new Promise(function (resolve, reject) {
            mongo.MongoClient.connect(connectionString, function (err, db) {
                if (err !== null) {
                    reject(err);
                } else {
                    resolve(db);
                }
            });
        });
    };

}());
