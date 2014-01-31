Ming
====

A Quasi-RESTful Web Service for MongoDB.

Status
------

Experimental, not well tested.

Installation
------------

- Run `sudo npm install ming -g`
- Run `ming` (see Usage)
- Put an SSL-enabled proxy in front of it

Usage
-----

- `port`: Ming Server port (default *1337*)
- `connection-string`: MongoDB [Connection String](http://docs.mongodb.org/manual/reference/connection-string/) for the default deployment (default *mongodb://localhost/ming*)

### Example

    ming --connection-string mongodb://mymongoserver/mymongodb

Clients
-------

- [Ming Fu](https://bitbucket.org/agrueneberg/ming-fu)

API
---

### Routes

#### Documents

- `GET /` retrieve collection names
- `GET /:collection` retrieve collection statistics
- `GET /:collection/:document` retrieve document
- `GET /:collection/:document/:field` retrieve field of document
- `POST /:collection/query` query documents
- `POST /:collection` create document
- `PUT /:collection/:document` update document
- `DELETE /:collection/:document` delete document

#### Files (GridFS)

- `GET /:prefix.files/:file` retrieve metadata of file
- `GET /:prefix.files/:file?binary=true` retrieve contents of file
- `POST /:prefix.files` create file
- `DELETE /:prefix.files/:file` delete file

### Headers

- `Authorization` basic HTTP authorization (e.g. in JavaScript `"Authorization : Basic " + btoa(username + ":" + password)`)
