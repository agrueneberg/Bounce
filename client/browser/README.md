Bounce Browser Client
=====================

Setup
-----

    <script src="bounce.js"></script>

Initialization
--------------

    db = bounce({
        deployment: "myDeployment",
        username: "myUsername",
        password: "myPassword"
    });

Methods
-------

### Get Resource

    db.getResource("/resource", function (err, resource) {
     // ...
    });

### Update Resource

    db.updateResource("/resource", {
        field: "value"
    }, function (err) {
     // ...
    });

### Delete Resource

    db.deleteResource("/resource", function (err) {
     // ...
    });

### Add Resource

#### JSON Documents

    db.addResource("/resource", {
        field: "value"
    }, function (err, id) {
     // ...
    });

#### Blobs

    db.addResource("/resource", blob, contentType, function (err, id) {
     // ...
    });

### Query Resource

    db.queryResource("/resource", {
        field: "value"
    }, function (err, results) {
     // ...
    }

### Get Permissions

    db.getPermissions("/resource", function (err, permissions) {
     // ...
    });

### Update Permissions

    db.updatePermissions("/resource", {
        operator: {
            user: state
        }
    }, function (err) {
     // ...
    });
