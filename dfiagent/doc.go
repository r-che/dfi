/*

dfiagent is a service that monitors changes of file system objects in real time.

Detected changes are reported to the database, where they become available to
the dfi utility. This provides the ability to fast search for files/directories
on distributed system using a single interface.

Note: for help with searching, see the [dfi utility] reference.

[dfi utility]: https://pkg.go.dev/github.com/r-che/dfi/cmd/dfi/

# Configuration

Most of the configuration of dfiagent is done by command line options.

Required Options:

  * --indexing-paths - comma-separated list of paths for indexing
  * --dbhost - database host or IP address and port in a DBMS-specific format
  * --dbid - database identifier - name, number and so on

By default, dfiagent attempts to connect to the database without authentication.

If authentication is required, you must specify the --db-priv-cfg option,
specifying the path to the file with authentication data (username, password,
etc.). The authentication data file is in JSON format. The specific format of
its contents depends on the used DBMS.  For details, see the database
driver-specific information in the dbi/{DBMS-name} directory.

For for more information about configuration options please run:

  dfiagent --help

# Example of how to run

  dfiagent \
      --checksums \
      --max-checksum-size 1048576 \
      --indexing-paths "/data/images,/data/multimedia,/data/backups" \
      --dbhost "mongodb://127.0.0.1:27017" \
      --dbid dfi  \
      --log-file /var/log/dfiagent.log

Explanation of the startup options:

  * --checksums - checksums will be calculated for regular files, this will make it possible to search for
    duplicate files and search by checksums
  * --max-checksum-size 1048576 - maximum size of files used for checksum calculation
  * --indexing-paths "/data/images,/data/multimedia,/data/backups" - list of directories to watch and index
  * --dbhost "mongodb://127.0.0.1:27017" - database server connection string
  * --dbid dfi - database name
  * --log-file /var/log/dfiagent.log - log file

At first startup the --reindex option may be useful - in this case it will
automatically start indexing the directories specified by --indexing-paths.
However, do not keep it in the list of options to run the resident service,
because in this case each restart will start reindexing, which can take a long
time and cause a heavy I/O and CPU load.

To start reindexing by a resident process, use sending a proper signal to it. See Signals handling section.

# Signals handling

  * TERM, INT - stop application
  * HUP - reopen the log file, useful for the log rotation procedure
  * USR1 - run reindexing
  * USR2 - run cleanup
  * QUIT - stop long-term operations such reindexing, cleanup, etc.


*/
package main
