(function () {
    "use strict";

    var mongo, Q;

    mongo = require("mongodb");
    Q = require("q");

    module.exports = function (connectionString) {
        var deferred = Q.defer();
        mongo.MongoClient.connect(connectionString, function (err, db) {
            if (err !== null) {
                deferred.reject(err);
            } else {
                deferred.resolve(db);
            }
        });
        return deferred.promise;
    };

}());
