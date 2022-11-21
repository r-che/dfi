dfi - utility to work with the Distributed File Indexer
==========

[![Go Reference](https://pkg.go.dev/badge/github.com/r-che/dfi/cmd/dfi/.svg)](https://pkg.go.dev/github.com/r-che/dfi/cmd/dfi/)

dfi utility is a command line interface to work with the Distributed File Indexer.

It supports searching for file system objects across many hosts, categorizing
file objects using tags and assigning text descriptions to specific objects.

## Available search features

File system objects can be searched by criteria:

 * Names and fragments of the full path, including full-text search (the result depends on the used DBMS)
 * Size and modification time of the object, search by ranges and sets of values is possible
 * Type - file, directory, symbolic link
 * Host where the object is located
 * File checksum
 * File identifier to search for duplicates
 * Additional information items values (tags, descriptions)

-------------------------
## Installation

Installation from source code only is currently available.

Clone the project repository:

```bash
git clone https://github.com/r-che/dfi.git
```

Change directory to the repository root and run build and/or installation:
```bash
cd dfi
go build -tags dbi_${BACKEND} ./cmd/dfi
go install -tags dbi_${BACKEND} ./cmd/dfi
```

Where `${BACKEND}` is:

  * `mongo` - to use MongoDB as a DBMS backend
  * `redis` - to use Redis as a DBMS backend

-------------------------
## Configuring database access

Before using the dfi utility, you must configure access to the same database that is used by [dfiagent].

[dfiagent]: https://github.com/r-che/dfi/dfiagent

### Redis

You need to create a file ~/.dfi/cli.json containing similar content:

```json
{
    "DB": {
        "HostPort": "${REDIS_HOST}:${REDIS_PORT}",
        "ID": "0"
    }
}
```

Where `${REDIS_HOST}` is the host or IP address of the Redis server, `${REDIS_PORT}` is the service port, 6379 usually.

#### Authentication

If your Redis server requires authentication, you have to configure ACL for the dfi user using the redis-cli utility:

```
ACL SETUSER dfi on >${REDIS_PASSWORD} resetkeys ~obj-meta-idx -@all +FT.SEARCH
  ~obj:* +scan +hget ~aii:* +hset +hget +hkeys +hdel +del +hgetall ~aii-idx +FT.SEARCH
  ~aii-meta:* +sadd +srem +smembers
```

<u>Notes</u>:

  * You need to enter the command as a single line, because Redis does not support line breaks in commands
  * The `>` character before `$PASSWORD` are important!

Then, you need to add `PrivCfg` section to the ~/.dfi/cli.json file:
```json
{
    "DB": {
        "HostPort": "${REDIS_HOST}:${REDIS_PORT}",
        "ID":       "0",
        "PrivCfg": {
            "user":     "dfi",
            "password": "${REDIS_PASSWORD}"
        }
    }
}
```

### MongoDB

You need to create a file ~/.dfi/cli.json containing similar content:

```json
{
    "DB": {
        "HostPort": "mongodb://${MONGO_HOST}:{$MONGO_PORT}",
        "ID":       "dfi"
    }
}
```

Where `${MONGO_HOST}` is the host or IP address of the MongoDB server, `${MONGO_PORT}` is the service port, 27017 usually.

#### Authentication

If your MongoDB server requires authentication, you have to create a user and role using the mongosh utility:

```javascript
use admin

// Create dfi user role
db.createRole({
    role: "dfi",
    privileges: [{
        resource: { db: "dfi", collection: "objs" },
        actions: [ "find" ]
    }, {
        resource: { db: "dfi", collection: "aii" },
        actions: [ "find", "insert", "remove", "update" ]
    }],
    roles: []
})

// Create agent user
db.createUser({
    user: "dfi",
    pwd: "${MONGO_PASSWORD}",
    roles: [
       { role: "dfi", db: "admin" },
    ]
})

```

Then, you need to add `PrivCfg` section to the ~/.dfi/cli.json file:

```json
{
    "DB": {
        "HostPort": "${MONGO_HOST}:${MONGO_PORT}",
        "ID": "0",
        "PrivCfg": {
            "AuthMechanism": "SCRAM-SHA-1",
            "Username":      "dfi",
            "Password":      "${MONGO_PASSWORD}"
        }
    }
}
```

<u>Note:</u> The `AuthMechanism` field value is just an example, you can use a other mechanism and other authentication options according to the [official reference].

[official reference]: https://www.mongodb.com/docs/manual/core/authentication/

-------------------------
## Usage examples

For examples of use, see the [package reference].

[package reference]: https://pkg.go.dev/github.com/r-che/dfi/cmd/dfi/

-------------------------

## Feedback

Feel free to open the [issue] if you have any suggestions, comments or bug reports.

[issue]: https://github.com/r-che/dfi/issues
