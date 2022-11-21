dfiagent - resident process of Distributed File Indexer
==========

[![Go Reference](https://pkg.go.dev/badge/github.com/r-che/dfi/dfiagent.svg)](https://pkg.go.dev/github.com/r-che/dfi/dfiagent)

dfiagent is a service that monitors changes of file system objects in real time.

Detected changes are reported to the database, where they become available to
the [dfi utility]. This provides the ability to fast search for files/directories
on distributed system using a single interface.

Note: for help with searching, see the dfi utility reference.

[dfi utility]: https://pkg.go.dev/github.com/r-che/dfi/cmd/dfi/

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
go build -tags dbi_${BACKEND} ./dfiagent
go install -tags dbi_${BACKEND} ./dfiagent
```

Where `${BACKEND}` is:

  * `mongo` - to use MongoDB as a DBMS backend
  * `redis` - to use Redis as a DBMS backend

-------------------------
## Configuration

Most of the configuration of dfiagent is done by command line options.
For for more information about configuration options see the [package reference] and the command help:

```bash
dfiagent --help
```

[package reference]: https://pkg.go.dev/github.com/r-che/dfi/dfiagent/

-------------------------
## Configuring database

### Database connection settings

To configure database connection you need to use the `--dbhost` and `--dbid` command line options.

For more details see `dfiagent --help` and the [package reference].

### Database authentication

By default, dfiagent attempts to connect to the database without authentication.

If authentication is required, you first need to create a database user.

#### Redis

You need to configure ACL for the dfiagent user using the redis-cli utility:

```
ACL SETUSER dfiagent on >${REDIS_PASSWORD} resetkeys ~obj:* -@all +scan +hset +del
```

Notes:

  * The `>` character before `${REDIS_PASSWORD}` are important!
  * `+hget` should be added if you want read-only DB mode can work properly

Then, you need to provide dfiagent the authentication configuration file using `--db-priv-cfg`.
The contents of the file should be as follows:

```json
{
	"user":	"${REDIS_USER}",
	"password": "${REDIS_PASSWORD}"
}
```


#### MongoDB

You need to to create a user and role using the mongosh utility:

```javascript
use admin

// Create agent role
db.createRole({
    role: "dfiagent",
    privileges: [{
        resource: { db: "dfi", collection: "objs" },
        actions: [ "find", "insert", "remove", "update" ]
    }],
    roles: []
})

// Create agent user
db.createUser({
    user: "${MONGO_USER}",
    pwd: "${MONGO_PASSWORD}",
    roles: [
       { role: "dfiagent", db: "admin" },
    ]
})
```

Then, you need to provide dfiagent the authentication configuration file using `--db-priv-cfg`.
The contents of the file should be as follows:

```json
{
	"AuthMechanism": "SCRAM-SHA-1",
	"Username":	"${MONGO_USER}",
	"Password": "${MONGO_PASSWORD}"
}
```
<u>Note:</u> The `AuthMechanism` field value is just an example, you can use a other mechanism and other authentication options according to the [official reference].

[official reference]: https://www.mongodb.com/docs/manual/core/authentication/

-------------------------
## Indices creation

To make a full-text search work, you must create the necessary indexes in the database.

### Redis

To create [Redis full-text] indices, you need to run the following commands using redis-cli utility:

```
SELECT 0

FT.CREATE obj-meta-idx ON HASH PREFIX 1 obj: LANGUAGE ${LANGUAGE} STOPWORDS 0 SCHEMA
    name TEXT WEIGHT 15
    id TAG fpath TEXT WEIGHT 10
    rpath TEXT WEIGHT 5
    host TAG
    type TAG
    size NUMERIC SORTABLE
    mtime NUMERIC SORTABLE
    csum TAG

FT.CREATE aii-idx ON HASH PREFIX 1 aii: LANGUAGE ${LANGUAGE} STOPWORDS 0 SCHEMA
    tags TAG
    descr TEXT
    oid TEXT NOINDEX
```

Where ${LANGUAGE} is the language used in the names of the file system objects.

<u>Notes:</u>

  * `FT.CREATE` can work only database 0
  * You need to enter the commands as a single line, because Redis does not support line breaks in commands

[Redis full-text]: https://redis.io/commands/ft.create/

### MongoDB ###

To create [MongoDB full-text] indices, you need to run the following commands using mongosh utility:

```javascript
// Full-text search field
db.objs.createIndex(
    {
        fpath:  "text",
        rpath:  "text",
        tfpath: "text",
        name:   "text",
        tname:  "text",
    },
    {
        // Increase weights of filenames related fields to --only-name option work properly
        weights: {
            name: 1000,
            tname: 1000,
        },
        default_language: "${LANGUAGE}",
    },
)
```

Where ${LANGUAGE} is the language used in the names of the file system objects.

To improve the performance of the database, the creation of additional indexes is recommended:

```javascript
// Index by host field
db.objs.createIndex({host: 1})

// Index by checksum field
db.objs.createIndex({csum: 1})
```

Note: It is up to you to experiment with the creation of additional indices.

[MongoDB full-text]: https://www.mongodb.com/docs/manual/core/index-text/

-------------------------
## Startup

For a startup example, list of handled OS signals, and so on, see the [package reference].

### OS configuration

On Linux, inotify (via the [fsnotify] package) is used to watch for changes of
filesystem objects. When watching paths with many nested directories, you may
get "**no space left on device**" or "**too many open files**" errors. In this case,
you need to configure the OS further, see [fsnotify-linux] for details.
(In short - you have to ajust fs.inotify.max_user_watches fs.inotify.max_user_instances)

[fsnotify]: https://github.com/fsnotify/fsnotify
[fsnotify-linux]: https://github.com/fsnotify/fsnotify#platform-specific-notes

-------------------------

## Feedback

Feel free to open the [issue] if you have any suggestions, comments or bug reports.

[issue]: https://github.com/r-che/dfi/issues
