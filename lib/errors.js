(function () {
    "use strict";

    var util, BadRequest, Forbidden, NotFound, Conflict;

    util = require("util");

    BadRequest = function (message) {
        this.message = message;
        this.statusCode = 400;
    };
    util.inherits(BadRequest, Error);

    Forbidden = function () {
        this.message = "You are not allowed to access this resource.";
        this.statusCode = 403;
    };
    util.inherits(Forbidden, Error);

    NotFound = function () {
        this.message = "The resource could not be found.";
        this.statusCode = 404;
    };
    util.inherits(NotFound, Error);

    Conflict = function (message) {
        this.message = message;
        this.statusCode = 400;
    };
    util.inherits(Conflict, Error);

    module.exports = {
        BadRequest: BadRequest,
        Forbidden: Forbidden,
        NotFound: NotFound,
        Conflict: Conflict
    };

}());
