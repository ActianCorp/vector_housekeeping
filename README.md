# Vector Housekeeping

Note that this program is still under development and not ready for Production usage yet. Testing assistance would be appreciated though !

This program is designed to be run frequently - as often as daily - and undertakes a number of housekeeping tasks for a Vector installation: 

- first it closes the installation to external usage by turning off the net servers - so don't run this during your online day !
- `modify to combine` on all databases to force updata propagation
- `modify to reconstruct` all non-Vector tables
- does **not** `modify to rewrite` Vectorwise table types by default as this could get very slow
- runs `optimizedb` on all tables
- check for data skew for all partitioned tables and reports this with an 'alert' message type
- for VectorH, checks whether partitioned tables have a #partitions isn't a multiple of the number of nodes and if so, reports this with an 'alert' message type
- checks whether there are very large, non-partitioned tables and alerts about these. 'Too large' is configurable, but defaults to 5% of the buffer pool size, or more.
- condenses the Vector LOG file
- sysmods the system catalogs for every database
- cleans up and deletes unused files for every database
- optionally backs up user databases (turned off by default)
- backs up iidbdb, keeping three backups (by default)
- then restarts external access again

It must be run only by the installation owner.

## Usage
`vector_housekeeping.sh <installation id> [list of databases to process]`

No parameters: print usage information

One param: Installation id of the installation to use. Assumes there is a .ingXXsh file to source for appropriate settings, or else that you are already in that installation.

Multiple params after installation id: optional list of databases within this installation to housekeep. If none provided, will work through all databases in installation except iidbdb and imadb


If you have table-specific modify scripts, place them in a folder named after the database with a <tablename>.sql file name extension.

If you have a table-specific optimizedb command-line, place it in the same folder and named matching the table name, but with a .opt file name extension and it will be used instead of the default operation.
Note that this must be the whole optimizedb command-line, not just the options.
The default optimizedb operation looks at key columns only (i.e. -zk) but also depends on the size of the table, fixing the max size of the histograms at 4000 cells for tables more than 30k rows.

BEWARE: default behaviour is to gather stats on all columns of all tables, so for really wide tables, this will take a long time. In cases like this, please adopt the table-specific method to pick out only columns used in WHERE clauses, using the -r <tablename> and -a <column name> flags to optimizedb.

Output gets logged to $HOUSEKEEPING_LOG, or else /tmp if this variable is not set.

## What housekeeping does it not do ?

It does not automatically housekeep the various log files in an installation. This is handled by the Liniux standard utility logrotate, same as all other log files on Linux, and a sample config file is supplied within this package. Just drop this config file into the logrotate config directory, usually /etc/logrotate.d.

It does not automatically turn on INFO level logging - but maybe it should. This makes it possible to see query times in vectorwise.log, as well as other information.

It does not automatically turn on global gathering of query profiles - but maybe it should.

It does not automatically turn on gathering of query trace information via trace point SC930 - but maybe it should.
