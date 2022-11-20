/*
Distributed File Indexer (dfi) package - provides a set of tools for indexing,
categorizing and searching file system objects on multiple hosts in real time.

# Key Features

  * Distributed work on multiple hosts
  * Updating information about discovered objects in real time
  * Fast search by the specified criteria based on the DBMS, rather than on the filesystems of particular hosts
  * Searching for duplicate files and files by a known checksum (sha1)
  * Categorizing indexed objects using tags
  * Text descriptions for objects

# Key Components

  * dfiagent - indexes objects on hosts and tracks their changes in real time
  * dfi - is a command line interface to work with the Distributed File Indexer.

See the corresponding subdirectories for more information about these components.

*/
package dfi
