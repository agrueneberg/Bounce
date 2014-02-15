(function () {
    "use strict";

    var util, BadRequest, Conflict, Forbidden, NotFound, UnsupportedMediaType;

    util = require("util");

    BadRequest = function (message) {
        this.message = message;
        this.statusCode = 400;
    };
    util.inherits(BadRequest, Error);

    Conflict = function (message) {
        this.message = message;
        this.statusCode = 400;
    };
    util.inherits(Conflict, Error);

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

    UnsupportedMediaType = function () {
        this.message = "The server only supports JSON formats.";
        this.statusCode = 415;
    };
    util.inherits(UnsupportedMediaType, Error);

    module.exports = {
        BadRequest: BadRequest,
        Conflict: Conflict,
        Forbidden: Forbidden,
        NotFound: NotFound,
        UnsupportedMediaType: UnsupportedMediaType
    };

}());
