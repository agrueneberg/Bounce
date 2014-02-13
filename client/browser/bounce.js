(function () {
    "use strict";

    var ajax, checkResource, detectJSON, bounce;

    ajax = function (options, callback) {
        var xhr, url;
        xhr = new XMLHttpRequest();
        url = options.url;
        Object.keys(options.params || {}).forEach(function (param, i) {
            if (i === 0) {
                url += "?";
            } else {
                url += "&";
            }
            url += param + "=" + encodeURIComponent(options.params[param]);
        });
        xhr.open(options.method, url);
        if (options.username && options.password) {
            xhr.setRequestHeader("Authorization", "Basic " + btoa(options.username + ":" + options.password));
        }
        Object.keys(options.headers || {}).forEach(function (name) {
            xhr.setRequestHeader(name, options.headers[name]);
        });
        xhr.responseType = options.responseType;
        xhr.onload = function () {
            var headers;
            if (xhr.status >= 200 && xhr.status < 300) {
                headers = {};
                xhr.getAllResponseHeaders().split("\r?\n").filter(function (responseHeader) {
                    return responseHeader !== "";
                }).forEach(function (responseHeader) {
                    var pair, name;
                    pair = responseHeader.split(": ");
                    name = pair[0].toLowerCase();
                    if (headers.hasOwnProperty(name) === false) {
                        headers[name] = pair[1].trim();
                    }
                });
                callback(null, {
                    status: xhr.status,
                    headers: headers,
                    body: xhr.response
                });
            } else {
                callback({
                    status: xhr.status,
                    message: xhr.statusText
                }, null);
            }
        };
        xhr.send(options.body || null);
    };

    checkResource = function (resource, callback) {
        if (typeof resource !== "string" || resource.length === 0 || resource.charAt(0) !== "/") {
            callback(new Error("resource has to be a string that starts with \"/\""));
        } else {
            callback(null);
        }
    };

    detectJSON = function (contentType) {
        var pattern;
        pattern = /^application\/([\w!#\$%&\*`\-\.\^~]*\+)?json$/i;
        return pattern.test(contentType);
    };

    bounce = function (options) {
        options = options || {};
        if (options.hasOwnProperty("deployment") === false) {
            throw new Error("Please provide a 'deployment' parameter.");
        } else {
         // Trim trailing '/'.
            if (options.deployment.charAt(options.deployment.length - 1) === "/") {
                options.deployment = options.deployment.substring(0, options.deployment.length);
            }
        }
        return {
            getResource: function (resource, callback) {
                checkResource(resource, function (err) {
                    var queryParams, isBinary;
                    if (err !== null) {
                        callback(err);
                    } else {
                        queryParams = resource.split("?")[1];
                        if (queryParams !== undefined) {
                            isBinary = queryParams.split("&").some(function (queryParam) {
                                return queryParam === "binary=1";
                            });
                        } else {
                            isBinary = false;
                        }
                        ajax({
                            method: "GET",
                            url: options.deployment + resource,
                            responseType: isBinary === true ? "blob" : "json",
                            username: options.username,
                            password: options.password
                        }, function (err, res) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                callback(null, res.body);
                            }
                        });
                    }
                });
            },
            updateResource: function (resource, body, callback) {
                checkResource(resource, function (err) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        ajax({
                            method: "PUT",
                            url: options.deployment + resource,
                            headers: {
                                "Content-Type": "application/json"
                            },
                            body: JSON.stringify(body),
                            username: options.username,
                            password: options.password
                        }, function (err) {
                            callback(err);
                        });
                    }
                });
            },
            deleteResource: function (resource, callback) {
                checkResource(resource, function (err) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        ajax({
                            method: "DELETE",
                            url: options.deployment + resource,
                            username: options.username,
                            password: options.password
                        }, function (err) {
                            callback(err);
                        });
                    }
                });
            },
            addResource: function (resource, body, contentType, callback) {
                checkResource(resource, function (err) {
                    if (err !== null) {
                        callback(err);
                    } else {
                     // Assume the body is JSON if no other contentType has been passed.
                        if (typeof contentType === "function") {
                            callback = contentType;
                            contentType = "application/json";
                            body = JSON.stringify(body);
                        }
                        ajax({
                            method: "POST",
                            url: options.deployment + resource,
                            headers: {
                                "Content-Type": contentType
                            },
                            body: body,
                            username: options.username,
                            password: options.password
                        }, function (err, res) {
                            if (err !== null) {
                                callback(err, null);
                            } else {
                                callback(null, res.headers.location);
                            }
                        });
                    }
                });
            },
            queryResource: function (resource, query, opts, callback) {
                checkResource(resource, function (err) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        if (typeof opts === "function") {
                            callback = opts;
                            opts = {};
                        }
                        ajax({
                            method: "POST",
                            url: options.deployment + resource + "/query",
                            params: opts,
                            headers: {
                                "Content-Type": "application/json"
                            },
                            body: JSON.stringify(query),
                            responseType: "json",
                            username: options.username,
                            password: options.password
                        }, function (err, res) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                callback(null, res.body);
                            }
                        });
                    }
                });
            },
            getPermissions: function (resource, callback) {
                checkResource(resource, function (err) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        ajax({
                            method: "GET",
                            url: options.deployment + "/.well-known/governance?resource=" + resource,
                            responseType: "json",
                            username: options.username,
                            password: options.password
                        }, function (err, res) {
                            if (err !== null) {
                                callback(err);
                            } else {
                                callback(null, res.body);
                            }
                        });
                    }
                });
            },
            updatePermissions: function (resource, body, callback) {
                checkResource(resource, function (err) {
                    if (err !== null) {
                        callback(err);
                    } else {
                        ajax({
                            method: "PUT",
                            url: options.deployment + "/.well-known/governance?resource=" + resource,
                            headers: {
                                "Content-Type": "application/json"
                            },
                            body: JSON.stringify(body),
                            username: options.username,
                            password: options.password
                        }, function (err) {
                            callback(err);
                        });
                    }
                });
            }
        };
    };

    window.bounce = bounce;

}());
