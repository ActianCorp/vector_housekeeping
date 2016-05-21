#!/bin/bash
#
# Copyright 2016 Actian Corporation
#
# Program Ownership and Restrictions.
#
# This Program (Shell Script) provided hereunder is licensed, not sold, and all
# intellectual property rights and title to the Program shall remain with Actian
# and Our suppliers and no interest or ownership therein is conveyed to you.
#
# No right to create a copyrightable work, whether joint or unitary, is granted
# or implied; this includes works that modify (even for purposes of error
# correction), adapt, or translate the Program or create derivative works, 
# compilations, or collective works therefrom, except as necessary to configure
# the Program using the options and tools provided for such purposes and
# contained in the Program. 
#
# The Program is supplied directly to you for use as defined by the controlling
# documentation e.g. a Consulting Agreement and for no other reason.  
#
# You will treat the Program as confidential information and you will treat it
# in the same manner as you would to protect your own confidential information,
# but in no event with less than reasonable care.
#
# The Program shall not be disclosed to any third party (except solely to
# employees, attorneys, and consultants, who need to know and are bound by a
# written agreement with Actian to maintain the confidentiality of the Program
# in a manner consistent with this licence or as defined in any other agreement)
# or used except as permitted under this licence or by agreement between the
# parties.
#

#----------------------------------------------------------------------------
#
# Name:
#   vector_housekeeping.sh 
#
# Parameters:             Installation ID         
#                         Database list [optional]
#
# Description:
#   This program undertakes a number of housekeeping tasks within the installation
#   and database(s) requested.
#   See the Usage report below for details of what it does.
#
#   Note: There are quotes around pathnames everywhere to make sure that it works ok on a Windows
#   Vector installation where there are likely spaces in path names.
#
#
# History:
#   1.0 07-Jan-2016 (sean.paton@actian.com)
#       Original version.
#   1.1 07-May-2016 (david.postle@actian.com)
#       Added table-skew detection, partition checks, and made Windows-compatible.

# TODO
# Could make elements of this switchable to daily/weekly operations via param file.
# Could split out the database operations to allow them to be run by the DBA, not just the installation
# owner.
# Could count how many update propagation events happened today, and warn user if more than one or two.
# Could also count how many LOG condense operations happened automatically and suggest a larger log file 
# if there were lots of them.

# Set up some high-level params here to make them easier to modify if needed.
# What ratio is too big for smallest partition to largest ?
SKEW_THRESH=5

# Always backup iidbdb, but how many backups of the master database should we keep ?
IIDBDB_BACKUPS_RETAIN=3

# What is the largest percentage of the bufferpool for a non-partitioned table before we alert ?
MAX_NP_BLOCKS_PCT=5

# Should we back up non-system databases, and if so, how many to keep ?
BACKUP_USER_DATABASES=0
USER_DATABASE_BACKUP_RETAIN=3

# Auto-enable query profiling for all queries ? Default is to allocate up to 100Mb for these
AUTO_ENABLE_PROFILING=0
PROFILE_PATH=$TEMP

MESSAGE ()
{
    # Quit if the message is flagged as a Fatal Error, otherwise keep going
    # Most errors should not prevent further housekeeping from continuing
	echo "${DBNAME} : `date +"${DATE}"` : $*"

    # If the message is an alert, could flag that up for more visible alerting, e.g. via email
	if [ "${1}" == "FATAL" ]
    then
        # If there is a problem and we already shut down the net servers, try to restart them before exiting
        if [ $CLOSED_INSTALLATION -eq 1 ]
        then
            ingstart -iigcd 
            ingstart -iigcc
        fi

        # Remove the control flag so we can re-run housekeeping again in the event of an error
        rm "${HOUSEKEEPING_LOG}/vector_housekeeping_control${ID}.pid"

        exit 1
    fi
}

#---------------------------------------------------------------------------#

DATE="%d-%b-%y %H:%M:%S"

if [[ $# -lt 1 ]]
then
    echo "Usage: `basename $0` INSTALLATION_ID [List of Databases]"
    echo
    echo "This program undertakes a number of housekeeping tasks for the named Vector installation"
    echo "including: 'modify to combine' on all databases, reconstruct all non-Vector tables, "
    echo "optimize all tables, check for data skew for all partitioned tables, condense the LOG "
    echo "file, sysmod the system catalogs, cleanup unused files, check for large tables that are"
    echo "not partitioned but really should be, checks that partitioned tables are a multiple of the"
    echo "number of nodes (for VectorH only), and finally backs up iidbdb, "
    echo "keeping three backups (by default)."
    echo
    echo "It must be run only by the installation owner."
    echo
    echo "If you have table-specific modify scripts, place them in a folder named after the "
    echo "database with a <tablename>.sql file name extension."
    echo
    echo "If you have a table-specific optimizedb command-line, place it in the same folder but "
    echo "with a .opt file extension and it will be used instead of the default operation."
    echo "Note that this must be the whole optimizedb command-line, not just the options."
    echo "The default optimizedb operation depends on the size of the table."
    echo
    echo "Output gets logged to \$HOUSEKEEPING_LOG, or else /tmp if this variable is not set."

    exit 1
fi

export CLOSED_INSTALLATION=0

ID="$1"
MESSAGE="Setting installation variables for Installation ${ID} from ~/.ing${ID}sh"
shift

if [ -f $HOME/.ing${ID}sh ]
then
   source $HOME/.ing${ID}sh || MESSAGE ERROR $MESSAGE
elif [ "$II_SYSTEM" != "" ]
then
    # No environment file, so see if we are already in this installation, otherwise quit

    if [ `ingprenv | grep II_INSTALLATION | cut -d= -f2` != "$ID" ]
    then
        MESSAGE="Requested housekeeping for installation $ID but cannot find that installation."
        MESSAGE FATAL $MESSAGE
    fi
else
    MESSAGE="Requested housekeeping for installation $ID but cannot find that installation."
    MESSAGE FATAL $MESSAGE
fi

INST_OWNER=`ls -ld "${II_SYSTEM}/ingres/bin" | awk '{print $3}'`
if [ `whoami` != "$INST_OWNER" ] ; then
    MESSAGE="ERROR: Only user $INST_OWNER may run this `basename $0` script."
    MESSAGE FATAL $MESSAGE
fi

if [[ -z $HOUSEKEEPING_LOG ]]
then
    # Default the log location
    export HOUSEKEEPING_LOG=/tmp
fi

if [[ -f "${HOUSEKEEPING_LOG}/vector_housekeeping_control${ID}.pid" ]]
then
    echo "FATAL: Script `basename $0` already running for installation $ID."
    exit 1
fi

MESSAGE="Cannot write pid to temp folder $HOUSEKEEPING_LOG."
echo $$ >"${HOUSEKEEPING_LOG}"/vector_housekeeping_control${ID}.pid || MESSAGE FATAL $MESSAGE

MESSAGE="Closing installation $ID to connections to begin housekeeping."
MESSAGE MESSAGE $MESSAGE

# Closing installation to external access while housekeeping is running
ingstop -iigcc -force
ingstop -iigcd -force
export CLOSED_INSTALLATION=1

if [ $# -gt 0 ]
then
	# Only processing some named databases, so build an index of these now from command line params.
    DBLIST=$@
else
    # Processing all databases, so get a list of these from infodb
    DBLIST=`infodb -databases | grep -v "iidbdb" | grep -v "imadb" `
fi

index=0
# Build array of these database names    
for DBNAME in $DBLIST
do
    DB[index]="${DBNAME}"
    ((index++))
done

# Is this a VectorH installation ?
if [ -f "$II_SYSTEM/ingres/files/slaves" ]
then
    NUM_NODES=`cat $II_SYSTEM/ingres/files/hdfs/slaves|wc -l`
    VECTORH=1
else
    NUM_NODES=1
    VECTORH=0
fi

export NUM_NODES
export VECTORH


DBNAME=""

# Process each database in our list
for DBNAME in ${DB[@]}
do
    TMPFILE="${HOUSEKEEPING_LOG}/tabledata_${DBNAME}.log"
    if [[ -f ${TMPFILE} ]]
    then
        rm "${TMPFILE}" 2> /dev/null
    fi

    # Don't delete the housekeeping log file, so we keep a record of what we've done
    HOUSEKEEPINGFILE="${HOUSEKEEPING_LOG}/vector_housekeeping_${DBNAME}.log"

    # Get the bufferpool size of this database. Used later for table size calculations, but 
    # the value is the same for the whole database so get it now.
    BUFFER_POOL_BYTES=`vwinfo -c ${DBNAME}|grep bufferpool_size | awk -F \| '{print $3}'`
    BLOCK_SIZE_BYTES=`vwinfo -c ${DBNAME}|grep block_size | awk -F \| '{print $3}'`
    
    # Calculate how many blocks a non-partitioned table can have in this database before
    # we consider it a bit too large for comfort.
    ((MAX_NP_BLOCKS=($BUFFER_POOL_BYTES*$MAX_NP_BLOCKS_PCT)/($BLOCK_SIZE_BYTES*100)))

    MESSAGE MESSAGE "Starting housekeeping for database ${DBNAME}"

    # First process the tables in the database. Get a list of these then loop round them.
    # Note that OPTPARAM needs to be last in the following list as it has a bunch of spaces in it
    # which messes up the space-based parsing.
    MESSAGE="Error creating table list. See Log at ${TMPFILE}"
    sql -s -v" " ${DBNAME} <<EOF >"${TMPFILE}" 2>&1 || MESSAGE ERROR $MESSAGE
\notitles
\notrim
SELECT
aim='dataline',
dba=DBMSINFO('DBA'),
table_name,
table_owner,
num_rows,
storage_structure,
phys_partitions,
partition_dimensions as partitioned_flag,
CASE
    when num_rows > 30001 and storage_structure like 'VECTOR%' then '-zr4000 -zu4000'
    else '-zk'
END as param
FROM iitables
WHERE table_type       = 'T'
AND num_rows           > 0
AND storage_structure != 'HEAP'
AND table_owner       != '\$ingres'
ORDER BY table_owner,num_rows;
\p\g
EOF
    grep "^ dataline" "${TMPFILE}" | while read ROWMARKER DBOWNER TABLE OWNER ROWS STRUCTURE PARTITIONS PARTITIONED OPTPARAM 
    do
    	# This section gets completed for every Vectorwise table in the current database
        if [[ ${STRUCTURE} == "VECTORWISE" ]]
        then
       	   # Look for database and table-specific config file, and use it if present.
           # If not, do nothing to the structure of the table. Combine for all tables comes later.
           # TODO
           # Should really have more of a path prefix here than a purely relative one - too dependent
           # on where this script is called from.
           if [[ -f ${DBNAME}/${TABLE}.sql ]]
           then
           		$MESSAGE="Running table-specific modify script for $TABLE"
           		MESSAGE MESSAGE $MESSAGE
           		sql -s -v" " ${DBNAME} -u$OWNER <${DBNAME}/${TABLE}.sql >>"${HOUSEKEEPINGFILE}" 2>&1 || MESSAGE ERROR ${MESSAGE}
           fi
        else
           # Ingres table structure, so rewrite it to reclaim deleted space in the table.

           MESSAGE="Modifying ${TABLE} to reconstruct."
           MESSAGE "MESSAGE" $MESSAGE
           echo "modify ${TABLE} to RECONSTRUCT;\p\g" > "${HOUSEKEEPING_LOG}"/reconstruct_${TABLE}.sql
           sql -s -v" " ${DBNAME} -u$OWNER <"${HOUSEKEEPING_LOG}"/reconstruct_${TABLE}.sql >>"${HOUSEKEEPINGFILE}" 2>&1 || MESSAGE ERROR ${MESSAGE}
           rm "${HOUSEKEEPING_LOG}"/reconstruct_${TABLE}.sql
        fi 

        # Clear out stats before re-generating them.
        MESSAGE="Removing Statistics From Table (T=${TABLE} O=${OWNER})"
        statdump -zdl -u${OWNER} ${DBNAME} -r${TABLE} 1>/dev/null || MESSAGE WARNING $MESSAGE
       
        # DP: Need to adjust the following because currently it uses key columns to optimize big tables.
        # If there are none, this is a bit useless. Need to be able to have table-specific optimize parms
        # Need a different mechanism to figure out which columns to optimize though.
        MESSAGE="Optimizing Table (T=${TABLE} O=${OWNER} R=${ROWS} S=${STRUCTURE} P=${OPTPARAM})"

        if [[ -f ${DBNAME}/${TABLE}.opt ]]
        then
            $MESSAGE="Running table-specific optimization script for $TABLE"
            MESSAGE MESSAGE $MESSAGE
            sh ${DBNAME}/${TABLE}.opt >>"${HOUSEKEEPINGFILE}" 2>&1 || MESSAGE ERROR $MESSAGE
        else
            optimizedb ${OPTPARAM} -u${OWNER} ${DBNAME} -r${TABLE} >>"${HOUSEKEEPINGFILE}" 2>&1 || MESSAGE ERROR $MESSAGE
        fi

        echo $DBOWNER> "${HOUSEKEEPING_LOG}"/DBOWN.dat

        if [ "$PARTITIONED" -gt 0 ]
        then
            # This is a partitioned table, so check for data skew and advise user if we find it.
            SKEW=0
            # Check for data skew on this table, and report it if we find it.
            SKEW=`echo "  SELECT FIRST 1 '#' + CHAR(INT(max_num_rows/num_rows)) as variation_ratio
                          FROM  (SELECT partition_id, 
                                        num_rows, 
                                        cast(max(num_rows) over () as float8) as max_num_rows
                                 FROM (SELECT   tid/10000000000000000 AS partition_id,
                                                count(*) as num_rows 
                                       FROM ${TABLE}
                                       GROUP BY 1
                                      ) X
                                ) Y
                          ORDER BY 1 DESC;\g" | sql -v" " ${DBNAME} |grep '^ #' |tr -d '#'`
            
            # Trim whitespace from Skew value for neater printing
            SKEW=`echo $SKEW | xargs`
            if [ ${SKEW} -ge $SKEW_THRESH ]
            then
                # This means that the largest partition has at least 5 times the data of the smallest
                MESSAGE="Data skew alert for table ${TABLE} which has a skew of ${SKEW}. Consider a new key."
                MESSAGE ALERT $MESSAGE
            else
                MESSAGE="Skew of $SKEW for table $TABLE is less than threshold of $SKEW_THRESH."
                MESSAGE MESSAGE $MESSAGE
            fi

            # Is the number of partitions a multiple of the number of nodes ?
            # Multiply by 10 to avoid Bash rounding down floating point numbers
            ((NUM_PARTS=$PARTITIONS%$NUM_NODES))
            if [[ $NUM_PARTS -ne 0 ]]
            then
                # No, it's not a multiple - so warn about this.
                MESSAGE="Table $TABLE is partitioned, but not on a multiple of the node count."
                MESSAGE ALERT $MESSAGE
            fi
        else
            # Look for large tables that are not partitioned, but ought to be. Only applies to 
            # VectorH tables - partitioning makes no real performance difference with Vector.
            if [ $VECTORH -eq 1 ]
            then
                # How many blocks does this table contain ?
                BLOCKS=`vwinfo -T ${TABLE} ${DBNAME} | awk -F \| '/${TABLE}/ {print $4}' | xargs`
                if [ $BLOCKS -gt $MAX_NP_BLOCKS ]
                then
                    MESSAGE="Table $TABLE has $BLOCKS VectorH blocks, but this is larger than $MAX_NP_BLOCKS_PCT% of buffer pool size "
                    MESSAGE+="of total size $BUFFER_POOL_BYTES bytes, and so table should be partitioned."
                    MESSAGE ALERT $MESSAGE
                fi
            fi
        fi

        # Note that we don't look for really small tables that are partitioned needlessly since
        # the overhead of this is not bad enough to be worth bothering about.
    done

    DBOWN=`cat "${HOUSEKEEPING_LOG}"/DBOWN.dat`
    MESSAGE="Running Modify to Combine for the whole database."
    MESSAGE MESSAGE $MESSAGE
    sql -s -v" " ${DBNAME} -u${DBOWN} <<EOF >>"${HOUSEKEEPINGFILE}" 2>&1 || MESSAGE ERROR $MESSAGE
call vectorwise (COMBINE);\p\g
EOF

	MESSAGE="Condensing the Vector LOG file"
	MESSAGE MESSAGE $MESSAGE
    sql -s -v" " ${DBNAME} -u${DBOWN} <<EOF >>"${HOUSEKEEPINGFILE}" 2>&1 || MESSAGE ERROR $MESSAGE
call vectorwise (CONDENSE_LOG);\p\g
EOF

    MESSAGE="Running sysmod ${DBNAME}"
    MESSAGE "MESSAGE" $MESSAGE
    sysmod ${DBNAME} >>"${HOUSEKEEPINGFILE}" || MESSAGE ERROR $MESSAGE

    MESSAGE="Cleaning up unused files for ${DBNAME}"
    MESSAGE MESSAGE $MESSAGE

    # Removed unused files left behind from last time, prior to possibly creating new ones next.
    # This is needed because occasionally files get left behind and 'orphaned' by Vector
    rm "$II_SYSTEM"/ingres/data/vectorwise/${DBNAME}/CBM/unused_* 2>/dev/null
    sql -s -v" " ${DBNAME} -u${DBOWN} <<EOF >>"${HOUSEKEEPINGFILE}" 2>&1 || MESSAGE ERROR $MESSAGE
call vectorwise (CLEANUP_UNUSED_FILES);\p\g
EOF

    # Backup this database, if requested
    if [ $BACKUP_USER_DATABASES -eq 1 ]
    then
        MESSAGE="Backing up database $DBNAME"
        ckpdb -keep=$USER_DATABASE_BACKUP_RETAIN ${DBNAME} >>"${HOUSEKEEPINGFILE}" || MESSAGE ERROR $MESSAGE
    fi

    # Check for log condensation activities and report if there have been a lot ?
    # Check for update propagation activities and report if a lot ?
    # Run a daily log analysis from here to gather query summary data ?
    # Check the vwinfo analysis for the above ?
    # Also run the lencheck scripts here to advise of poor schema data type choices ?
    rm "${TMPFILE}" 2> /dev/null
    rm "${HOUSEKEEPING_LOG}"/DBOWN.dat 2>/dev/null
done

# Turn on query profiling if requested. Note that this is an installation-wide setting, not per-database.
# This should work fine with a default vectorwise.conf, but may be dangerous if user has already modified it
# because we could end up with a duplicated [server] entry.
# Also, Vector needs to be restarted for this change to take effect. Default housekeeping doesn't restart the server.
if [ $AUTO_ENABLE_PROFILING -eq 1 ]
then
    MESSAGE="Changing vectorwise.conf settings to enable profiling for database $DBNAME"
    if [ `grep profile_per_query "$II_SYSTEM"/ingres/data/vectorwise/vectorwise.conf | wc -l` -eq 0 ]
    then
        # Use print here instead of echo since it's easier to embed newlines and do it all in one go
        print "%s" "[server]\nprofile_per_query=true\nprofile_per_query_dir=$PROFILE_PATH" >>"$II_SYSTEM"/ingres/data/vectorwise/vectorwise.conf 2>"${HOUSEKEEPINGFILE}" || MESSAGE ERROR $MESSAGE
    else
        # Config file already has a profile_per_query setting so need to flip this to true if it's false
        if [ `grep 'profile_per_query=false' "$II_SYSTEM"/ingres/data/vectorwise/vectorwise.conf | wc -l` -eq 0 ]
        then
            # TODO: edit in-place on vectorwise.conf file
        fi
    fi
fi

DBNAME=iidbdb
HOUSEKEEPINGFILE="${HOUSEKEEPING_LOG}/vector_housekeeping_${DBNAME}.log"

MESSAGE="Running sysmod of the Master database, iidbdb"
MESSAGE MESSAGE $MESSAGE
sysmod iidbdb >>"${HOUSEKEEPINGFILE}"|| MESSAGE ERROR $MESSAGE

MESSAGE="Checkpointing the Master database, iidbdb, and keeping 3 checkpoints."
MESSAGE MESSAGE $MESSAGE
ckpdb -keep=$IIDBDB_BACKUPS_RETAIN iidbdb >>"${HOUSEKEEPINGFILE}" || MESSAGE ERROR $MESSAGE

# All done, so re-open installation to users again
# Note: if on Windows, the following two lines will give an error which is benign, since these
# processes don't exist on Windows. So don't check for errors here.
ingstart -iigcd 
ingstart -iigcc
MESSAGE MESSAGE "Finished housekeeping for Vector databases in installation $ID."

rm "${HOUSEKEEPING_LOG}"/vector_housekeeping_control${ID}.pid

exit 0