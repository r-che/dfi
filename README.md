Distributed File Indexer
==========

Distributed File Indexer (dfi) - set of tools for indexing, categorizing and searching
file system objects on multiple hosts in real time.

## The main idea

Imagine that you ...

Know that you have the file you need, but you do not remember the directory
where it is located, or its exact name, or even the host where it might be
located. You only remember what its name might be, or approximately its size,
or approximately its date of change.

You have a sample file, and you need to know if there is the exact same file in
your file collection, perhaps with a completely different name and stored in an
unpredictable location.

You want to find all photos taken within a certain time interval.

You decide to clean up your file storages and find huge files to check for
duplicates.

After cleaning the storages, you decide to categorize the most important files
and provide descriptions for the most interesting ones, but you don't want to
rearrange the entire directory hierarchy for that.

And so on...

Of course, all these tasks can be done with a set of standard tools such as
find, locate, stat, grep, checksum utilities, ssh (to work with remote hosts).
However, this turns into a not very trivial task, in fact, developing
specialized software. This will also not be very efficient if you need to find
more than one file.

Distributed File Indexer was designed for simple and effective solution of such
problems.

## Key Features

  * Distributed work on multiple hosts.
  * Updating information about discovered objects in real time.
  * Fast search by specified criteria using a fast database rather than
    the slow file systems of multiple hosts.
  * Full-text search by names of file system objects, including search by full
    paths of objects (the result depends on the DBMS used).
  * Searching for duplicate files and files by a known checksum (sha1).
  * Categorizing indexed objects using tags.
  * Text descriptions for objects.

## Key Components

[dfiagent] - indexes objects on hosts and watches their changes in real time.

[dfi] - is a command line interface to work with the Distributed File Indexer.

DBMS, currently supported are:

  * Redis
  * MongoDB
  * _PostgreSQL is expected in the near future._

[dfiagent]: dfiagent/
[dfi]: cmd/dfi/

## Operating system support

Currently, tested only on Linux. However, there should be no fundamental problems in building for other operating systems.

-------------------------
## Resource consumption

Below are the resource consumption metrics for specific datasets.

### dfiagent

dfiagent memory consumption on different datasets:

* When watching about 5 thousand directories containing about 50,000 files, it consumes about 10-15 MB of RAM.
* When watching around 55 thousand directories containing around 700,000 files, it consumes about 100 MB of RAM.

<u>Note:</u> During full re-indexing, memory consumption may increase significantly.

CPU consumption should be taken into account only during re-indexing and bulk addition of new files - in this case the CPU is consumed for checksum counting, if such a feature is enabled.

### DBMS

With a database size of more than 800,000 objects:

  * Redis - consumes about 1.4 GB of memory. Keep in mind that the whole database is in memory, which on the one hand gives a very short response time, but on the other hand limits the amount of indexed data to the amount of RAM of the Redis server
  * MongoDB - consumes about 800 MB after active database operations, assuming there is a large amount of free memory in the OS.

-------------------------
## Caveats

  * Connection to databases is not encrypted.
  * Insufficient escaping of user-entered search phrases, which can lead to incorrect queries in the database.
  * In rare cases dfiagent may not correctly handle directory renaming.
  * Low tests coverage.

All the above-mentioned flaws are planned to be gradually eliminated.

-------------------------
## Feedback

Feel free to open the [issue] if you have any suggestions, comments or bug reports.

[issue]: https://github.com/r-che/dfi/issues
