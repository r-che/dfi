/*

dfi utility is a command line interface to work with the Distributed File Indexer.

Usage:

  dfi [Operating mode] { [Options] | [Search phrases...] }

# Available search features

File system objects can be searched by criteria:

 * Names and fragments of the full path, including full-text search (the result depends on the used DBMS)
 * Size and modification time of the object, search by ranges and sets of values is possible
 * Type - file, directory, symbolic link
 * Host where the object is located
 * File checksum
 * File identifier to search for duplicates
 * Additional information items values (tags, descriptions)

# Setting additional information items to objects

The following additional information items can be added to each indexed object:

  * Tags, to categorize objects
  * Detailed text description

# Formatting output

Search results, and information on specific objects (--show mode) can be displayed using:

  * Default human-readable format
  * Single-line key: "value" format, suitable for primitive parsers
  * JSON, with a deterministic field order, making it human-readable. JSON single-line output is also possible.

# Examples

Search by native properties of file system objects:

  # Search for all objects containing "sunrise" or "sunset" in the full object path:
  dfi sunrise sunset

  # Search for all objects with modification time before and including January 1, 1971:
  dfi --mtime ..1971.01.01

  # Search for all objects with modification time between January 1 and
  # December 31, 2006, included, and whose path contains "sunrise"
  dfi --mtime 2006.01.01..2006.12.31 sunrise

  # Search for all objects with a size of 10 GB or greater, print the object identifiers:
  dfi -i --size 10G..

Search for duplicates of object with ID b172..(cut)..45d8

  dfi --dupes b172..(cut)..45d8

Set additional information items (AII) for objects:

  # Set the "big-file" tag for objects with IDs 2a8a..(cut)..3add and a123..(cut)..ccaf
  dfi --set --tags big-file 2a8a..(cut)..3add a123..(cut)..ccaf

  # Set the description for one of the files:
  dfi --set --descr 'This is a huge file' 2a8a..(cut)..3add

  # Print information about the file with ID 2a8a...(cut)...3add
  dfi --show 2a8a...(cut)...3add

Search for additional information items of objects:

  # Search for all objects with tags or descriptions:
  dfi --aii-filled tags,descr

  # Search for objects with the big-file tag:
  dfi --only-tags big-file

# Configuration file

By default, dfi looks for the configuration file in ${HOME}/.dfi/cli.json.
You can specify path to the configuration file using --cfg option.

Configuration file uses JSON format. Currently it has only one section - DB,
that contains the information required to connect to the database:

  {
      "DB": {
          "HostPort": "${DB_HOST:DB_PORT}",
          "ID": "$DB_IDENTIFIER",
          "PrivCfg": {
              . . . DBMS specific private options ...
          }
      }
  }

The "HostPort" and "ID" fields must contain parameters for connecting to the
database server in a DBMS-specific format. The ID is a database name or
identifier, such as "dfi" for MongoDB or "0" for Redis.

The PrivCfg field contains information required for database authentication,
the format of its contents depends on DBMS. Without this field, dfi attempts
to connect to the database without using authentication. For details, see the
database driver-specific information in the dbi/{DBMS-name} directory.

# Additional help

For additional reference information, see:

  dfi --help
  dfi --docs [topic]

*/
package main
