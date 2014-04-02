Bounce
======

A RESTful governance layer for MongoDB.


Introduction
------------

* [Slides](https://docs.google.com/presentation/d/15GBuwu7Qw6DPQars77bxBk9fb7CyNj5p1CoqpIug1So)
* [Poster](http://www.iscb.org/images/stories/cshals2014/posters/Gruneberg-cshals2014.pdf) (presented at CSHALS 2014)


Getting Started
---------------

Clone this repository, start MongoDB, and run

    npm install
    ./bin/bounce --port 27080 --connection-string mongodb://localhost/bounce

### Database-level permissions

Database-level permissions are at the end of the inheritance chain and define the default behavior of Bounce. The default database-level permissions allow authenticated users to govern, read, write, and add their own data.

To change the database-level permissions, pass a reference to a file as part of the `database-permissions` option when starting Bounce. For example, to provide public read access, create a file called `public.json` with the following content

    {
        "rules": [{
            "operator": "govern",
            "role": "authenticated",
            "state": "all"
        }, {
            "operator": "read",
            "role": "public",
            "state": "all"
        }, {
            "operator": "write",
            "role": "authenticated",
            "state": "all"
        }, {
            "operator": "add",
            "role": "authenticated",
            "state": "all"
        }]
    }

and run

    ./bin/bounce --database-permissions public.json


Clients
-------

- [bounce.js](https://github.com/agrueneberg/Bounce/tree/master/client/browser) (for browsers)
- [Bouncer](https://github.com/drobbins/Bouncer) (for browsers, based on AngularJS)
- [bounce.json.postman_collection](https://github.com/agrueneberg/Bounce/tree/master/client/postman) (for Postman)
