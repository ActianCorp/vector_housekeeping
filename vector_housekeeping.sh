#!/bin/bash
#
# Copyright 2014 Actian Corporation
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
#
# History:
#   1.0 07-Jan-2016 (sean.paton@actian.com)
#       Original version.
#   1.1 07-May-2016 (david.postle@actian.com)
#       Added table-skew detection, and made Windows-compatible.

# TODO
# Could make elements of this switchable to daily/weekly operations via param file.
# Could split out the database operations to allow them to be run by the DBA, not the installation
# owner.

# Set up some high-level params here to make them easier to modify if needed.
# What ratio is too big for smallest partition to largest ?
SKEW_THRESH=5
# How many backups of the master database should we keep ?
IIDBDB_CKPS=3
# What is the largest percentage of the bufferpool for a non-partitioned table ?
MAX_NP_BLOCKS_PCT=5

MESSAGE ()
{
    # Quit if the message is flagged as an Error
	echo "${1} : ${DBNAME} : `date +"${DATE}"` : $*"
	if [ "${1}" == "ERROR" ]
    then
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
    echo "file, sysmod the system catalogs, cleanup unused files, and finally backup iidbdb, "
    echo "keeping three backups (by default)."
    echo
    echo "It must be run only by the installation owner."
    echo
    echo "If you have table-specific modify scripts, place them in a folder named after the "
    echo "database with a <tablename>.sql file name."
    echo
    echo "If you have a table-specific optimizedb command-line, place it in the same folder but "
    echo "with a .opt extension and it will be used instead of the default operation."
    echo "Note that this must be the whole optimizedb command-line, not just the options."
    echo "The default optimizedb operation depends on the size of the table."
    echo
    echo "Output gets logged to \$HOUSEKEEPING_LOG, or else /tmp if this variable is not set."

    exit 1
fi

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
        MESSAGE ERROR $MESSAGE
    fi
else
    MESSAGE="Requested housekeeping for installation $ID but cannot find that installation."
    MESSAGE ERROR $MESSAGE
fi

INST_OWNER=`ls -ld "${II_SYSTEM}/ingres/bin" | awk '{print $3}'`
if [ `whoami` != "$INST_OWNER" ] ; then
    MESSAGE="ERROR: Only user $INST_OWNER may run this `basename $0` script."
    MESSAGE ERROR $MESSAGE
fi

if [[ -z $HOUSEKEEPING_LOG ]]
then
    # Default the log location
    HOUSEKEEPING_LOG=/tmp
fi

if [[ -f "${HOUSEKEEPING_LOG}/vector_housekeeping_control${ID}.pid" ]]
then
    echo "ERROR: Script `basename $0` already running for installation $ID."
    exit 1
fi

MESSAGE="Cannot write pid to temp folder $HOUSEKEEPING_LOG."
echo $$ >"${HOUSEKEEPING_LOG}"/vector_housekeeping_control${ID}.pid || MESSAGE ERROR $MESSAGE

MESSAGE="Closing installation $ID to connections to begin housekeeping."
MESSAGE MESSAGE $MESSAGE

# Closing installation to external access while housekeeping is running
#ingstop -iigcc -force
#ingstop -iigcd -force

if [ $# -gt 0 ]
then
	# Only processing some named databases, so build an index of these now.
    DBLIST=$@
else
    # Processing all databases, so get a list of these
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
if [ -f $II_SYSTEM/ingres/files/slaves ]
then
    NUM_NODES=`cat $II_SYSTEM/ingres/files/hdfs/slaves|wc -l`
    VECTORH=1
else
    NUM_NODES=1
    VECTORH=0
fi


DBNAME=""

# Process each database in our list
for DBNAME in ${DB[@]}
do
    TMPFILE="${HOUSEKEEPING_LOG}/tabledata_${DBNAME}.log"
    if [[ -f ${TMPFILE} ]]
    then
        rm ${TMPFILE} 2> /dev/null
    fi

    HOUSEKEEPINGFILE="${HOUSEKEEPING_LOG}/vector_housekeeping_${DBNAME}.log"
    if [[ -f ${HOUSEKEEPINGFILE} ]]
    then
        rm ${HOUSEKEEPINGFILE}
    fi

    # Get the bufferpool size of this database. Used later for table size calculations, but 
    # the value is the same for the whole database so get it now.
    BUFFER_POOL_BYTES=`vwinfo -c ${DBNAME}|grep bufferpool_size | awk -F \| '{print $3}'`
    BLOCK_SIZE_BYTES=`vwinfo -c ${DBNAME}|grep block_size | awk -F \| '{print $3}'`
    
    # Calculate how many blocks a non-partitioned table can have in this database before
    # we consider it a bit too large for comfort.
    ((MAX_NP_BLOCKS = ($BUFFER_POOL_BYTES * $MAX_NP_BLOCKS_PCT) / ($BLOCK_SIZE_BYTES * 100))

    MESSAGE "MESSAGE" "Starting housekeeping for database ${DBNAME}"

    # First process the tables in the database. Get a list of these then loop round them.
    # Note that OPTPARAM needs to be last in the following list as it has a bunch of spaces in it
    # which messes up the space-based parsing.
    MESSAGE="Error creating table list. See Log at ${TMPFILE}"
    sql -s -v" " ${DBNAME} <<EOF >"${TMPFILE}" 2>&1 || MESSAGE WARNING $MESSAGE
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
    when num_rows between 0 and 30000 and storage_structure like 'VECTOR%' then '-zk'
    when num_rows between 30001 and 1000000 and storage_structure like 'VECTOR%' then '-zr4000 -zu4000'
    when num_rows > 1000001 and storage_structure like 'VECTOR%' then '-zr4000 -zu4000'
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
           		sql -s -v" " ${DBNAME} -u$OWNER <${DBNAME}/${TABLE}.sql >>"${HOUSEKEEPINGFILE}" 2>&1 || MESSAGE "ERROR" "${MESSAGE}"
           fi
        else
           # Ingres table structure, so rewrite it to reclaim deleted space in the table.

           MESSAGE="Modifying ${TABLE} to reconstruct."
           MESSAGE "MESSAGE" $MESSAGE
           echo "modify ${TABLE} to RECONSTRUCT;\p\g" > "${HOUSEKEEPING_LOG}"/reconstruct_${TABLE}.sql
           sql -s -v" " ${DBNAME} -u$OWNER <"${HOUSEKEEPING_LOG}"/reconstruct_${TABLE}.sql >>"${HOUSEKEEPINGFILE}" 2>&1 || MESSAGE "ERROR" "${MESSAGE}"
           rm "${HOUSEKEEPING_LOG}"/reconstruct_${TABLE}.sql
        fi 

        # Clear out stats before re-generating them.
        MESSAGE="Removing Statistics From Table (T=${TABLE} O=${OWNER})"
        statdump -zdl -u${OWNER} ${DBNAME} -r${TABLE} 1>/dev/null || MESSAGE WARNING "${MESSAGE}"
       
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
            # Look for large tables that are not partitioned, but ought to be. Only really applies to 
            # VectorH tables - partitioning makes no difference with Vector.
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

        # TODO: tell the user if we find a small table that is.
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
    sleep 10
    sysmod ${DBNAME} >>"${HOUSEKEEPINGFILE}" || MESSAGE ERROR $MESSAGE

    MESSAGE="Cleaning up unused files for ${DBNAME}"
    MESSAGE MESSAGE $MESSAGE

    # Removed unused files left behind from last time, prior to possibly creating new ones next.
    rm "$II_SYSTEM"/ingres/data/vectorwise/${DBNAME}/CBM/unused_* 2>/dev/null
    sql -s -v" " ${DBNAME} -u${DBOWN} <<EOF >>"${HOUSEKEEPINGFILE}" 2>&1 || MESSAGE ERROR $MESSAGE
call vectorwise (CLEANUP_UNUSED_FILES);\p\g
EOF

    # Check for log condensation activities and report if there have been a lot ?
    # Check for update propagation activities and report if a lot ?
    # Run the daily log analysis from here to gather query summary data ?
    # Check the vwinfo analysis for the above ?
    # Also run the lencheck scripts here to advise of poor schema data type choices ?
    rm "${TMPFILE}" 2> /dev/null
    rm "${HOUSEKEEPING_LOG}"/DBOWN.dat 2>/dev/null
done

MESSAGE="Running sysmod of the Master database, iidbdb"
MESSAGE MESSAGE $MESSAGE
sysmod iidbdb >>"${HOUSEKEEPINGFILE}"|| MESSAGE ERROR $MESSAGE

MESSAGE="Checkpointing the Master database, iidbdb, and keeping 3 checkpoints."
MESSAGE MESSAGE $MESSAGE
# TODO: Make the number of checkpoints configurable
# TODO: Consider moving iidbdb checkpoints out of daily script and into weekly ?
ckpdb -keep=$IIDBDB_CKPS iidbdb >>"${HOUSEKEEPINGFILE}" || MESSAGE ERROR $MESSAGE

# All done, so re-open installation to users again
# Note: if on Windows, the following two lines will give an error which is benign, since these
# processes don't exist on Windows. So don't check for errors here.
ingstart -iigcd 
ingstart -iigcc
MESSAGE MESSAGE "Finished housekeeping for Vector databases in installation $ID."

rm "${HOUSEKEEPING_LOG}"/vector_housekeeping_control${ID}.pid

exit 0