#!/bin/bash
# DBOPS-8743
# This script optimizes table online
# Author = vamsi krishna


# Functions

usage () {
	[[ ! -z $1 ]] && log "$1"

cat << EOF
	REQUIRED:
	==============
		- u : Username to connect to the host
		- p : Password for the username provided 
		- d : Database the table/s is/are on < Takes only one argument unlike -t > < Skips audit_log table>
		- t : Table/s to be optimized < multiple table names can be provided ONLY as COMMA SEPARATED VALUES >
			< When not specified, optimizes all tables in the provided database >
		- H : Hostname to run on < Defaults to localhost >
	
	OPTIONAL:
	==============
		- h : Prints this usage

	NOTES:
	==============
		1 ) This script does not work for tables that do not have a PRIMARY KEY. < Skips those tables >
		2 ) This script does not work for tables having foreign key constraints. 		 
EOF

exit 1
}

log () {
	echo "[`date +'%Y-%m-%d %H:%M:%S'`]: $1" | tee -a $LOGFILE
}

exit_check () {
        if [[ $? -ne 0 ]];then
                log "Error: $1"
                exit 1
	elif [[ ! -z $2 ]];then
		log "$2" 
        fi
}

continue_check () {
	if [[ $? -ne 0 ]];then
                log "Error: $1"
                continue 
        elif [[ ! -z $2 ]];then
                log "$2"
        fi
}

# Gets a list of tables when '-t' is not specified
get_tables () {
	local SQL="SELECT table_name FROM information_schema.tables WHERE table_schema='$DB' AND table_name != 'audit_log' AND table_type='BASE TABLE';"
	tbls=`$DB_CONN -ANBe"$SQL"`
	exit_check "Could not retrieve tables list for [$DB]. Exiting..."
	echo ${tbls}
}

# creates a new table to work on
create_new_table () {
	local SQL="CREATE TABLE ${table}_new LIKE $table;"
	log "Creating new table..."
	$DB_CONN -Ae"$SQL"
	continue_check "Could not create table [${table}_new]. Continuing..." "Created new table $DB.${table}_new [ OK ]"
}

# Generates a delete where clause statement for DELETE trigger 
get_stmt () {
	local SQL="select column_name from information_schema.columns where table_schema='$DB' and table_name='$table' and column_key='pri';"
	local keys=$($DB_CONN -ANBe"$SQL" | tr '\n' ' ')
	continue_check "Could not get the primary_key. Continuing..."
	local count=$(echo $keys | wc -w)
	local stmt=""
	local i=1
	if [[ $count -eq 0 ]];then
		return 1
	else
		while [[ $i -le $count ]];do
        	        key=$(echo $keys | cut -f${i} -d' ')
        	        if [[ -z $stmt ]];then
        	        	stmt="${table}_new.${key}=old.${key}"
        	        else
        	        	stmt="$stmt and ${table}_new.${key}=old.${key}"
        	        fi
        	        i=$[$i+1]
        	done
	fi
	del_stmt=$(echo $stmt)
}

# Generates a statement to be used for INSERT and UPDATE triggers
get_ins_stmt () {
	old_cols=`$DB_CONN -ANBe"SELECT column_name from information_schema.columns where table_schema='${DB}' and table_name='${table}'" | paste -s -d,`;
	continue_check "Could not get a list of columns. Continuing..."
	new_cols=`echo $old_cols | sed -e 's/,/\,new./g' -e 's/^/new./g'`

	echo "($old_cols) VALUES ($new_cols)"
}

# Checks if the triggers already exists and continues if so
check_triggers () {
	local SQL="select count(TRIGGER_NAME) from information_schema.TRIGGERS where TRIGGER_SCHEMA='$DB' and TRIGGER_NAME in ('${table}_ins','${table}_upd','${table}_del') ;"
	count=`$DB_CONN -ANBe"$SQL"`
	continue_check "Could not check if triggers exist already. Continuing..."
	if [[ $count -ne 0 ]];then
		return 1
	fi
}

# Creates DELETE, INSERT, UPDATE triggers
create_triggers () {
	check_triggers
	continue_check "Triggers already Exist. Please drop triggers before running the script again. Continuing..."
	get_stmt
	continue_check "No primary key found. Continuing..."
	local ins_stmt=$(get_ins_stmt)
	local upd_stmt=$(get_ins_stmt)

	DEL_TRIG=$(echo -e "
	DELIMITER // \n
        CREATE TRIGGER ${table}_del \n
                AFTER DELETE ON ${DB}.${table} \n
                FOR EACH ROW \n
                BEGIN \n
                DELETE IGNORE FROM ${DB}.${table}_new where ${del_stmt} ; \n
                END; // \n
	DELIMITER ; ")

	INS_TRIG=$(echo -e "
	DELIMITER // \n
	CREATE TRIGGER ${table}_ins \n
		AFTER INSERT ON ${DB}.${table} \n
		FOR EACH ROW \n
		BEGIN \n
		REPLACE INTO ${DB}.${table}_new ${ins_stmt} ; \n
		END; // \n
	DELIMITER ; ")

	UPD_TRIG=$(echo -e "
	DELIMITER // \n
	CREATE TRIGGER ${table}_upd \n
		AFTER UPDATE ON ${DB}.${table} \n
		FOR EACH ROW \n
		BEGIN \n
		REPLACE INTO ${DB}.${table}_new ${upd_stmt} ; \n
		END ; // \n
	DELIMITER ; ")

	log "Creating triggers..."
	$DB_CONN -e"$DEL_TRIG"
	exit_code=$?
	$DB_CONN -e"$INS_TRIG"
	exit_code=$(($exit_code+$?))
	$DB_CONN -e"$UPD_TRIG"
	exit_code=$(($exit_code+$?))

	if [[ $exit_code -ne 0 ]];then
		return 1
	elif [[ $exit_code -eq 0 ]];then
		return 0
	fi
}

# Copies the data to the new temporary table. Needs re-visit in next versions for better data copying procedures.
copy_data () {
	local SQL="SELECT column_name from information_schema.columns where table_schema='$DB' and table_name='${table}';"
	local cols=`$DB_CONN -ANBe"$SQL;" | paste -s -d,`
	continue_check "Could not get a list of columns for copying data. Continuing..."
	local INSERT_SQL="INSERT LOW_PRIORITY IGNORE INTO ${table}_new ($cols) SELECT $cols from ${table};"
	local count_sql="select count(*) from ${DB}.${table};"
	local count=`$DB_CONN -ANBe"$count_sql"`

	log "Copying approximately $count rows..."
	$DB_CONN -ANBe"${INSERT_SQL}"
	continue_check "Could not copy the data. Drop triggers and ${DB}.${table}_new table before running the script again. Continuing..." "Copied data [ OK ]"
}

# Swaps tables
rename_tables () {
	local SQL="RENAME TABLE ${DB}.${table} TO ${DB}.${table}_old, ${DB}.${table}_new TO ${DB}.${table};"
	log "Swapping tables..."
	$DB_CONN -ANBe"$SQL"
	continue_check "Could not swap tables. Continuing..." "Swapped original and new tables [ OK ]"
}

# Drops triggers
drop_triggers () {
	local SQL="DROP TRIGGER ${table}_ins; DROP TRIGGER ${table}_upd; DROP TRIGGER ${table}_del;"
	log "Dropping triggers..."
	$DB_CONN -ANBe"$SQL"
	if [[ $? -ne 0 ]];then
		log "Could not drop the triggers. Please drop the triggers manually."
		log "Also drop ${table}_old to clean crap generated by this script."
		continue
	else
		log "Dropped triggers [ OK ]"
	fi
}

# Drops the old table
drop_old_table () {
	local SQL="DROP TABLE ${DB}.${table}_old;"
	log "Dropping old table..."
	$DB_CONN -ANBe"$SQL"
	continue_check "Could not drop [${DB}.${table}_old]. Please drop manually. Continuing..." "Dropped table ${DB}.${table}_old [ OK ]"
}

# Validadtes the provided database
vaildate_db() {
	local SQL="select count(schema_name) from INFORMATION_SCHEMA.SCHEMATA where schema_name='${DB}';"
	count=`mysql -u$USER -p$PASS -ANBe"$SQL"`
	[[ $count -ne 1 ]] && log "[${DB}] not found on the host. Exiting..." && exit 1
}

### Get Options
while getopts "u:p:H:d:t:h" arg; do
        case "$arg" in
                u) USER="${OPTARG}";;
                p) PASS="${OPTARG}";;
                H) HOST="${OPTARG}";;
		d) DB="${OPTARG}";;
		t) TABLES="${OPTARG}";;
		h) HELP=1;; 
		*) usage;;
        esac
        allargs+=$OPTARG
done

[[ $HELP -eq 1 ]] && usage

# Grab the hostname initially to default to localhost if not specified as argument
host=$(hostname -s | sed 's/^mslvl//')

# Variables
LOGFILE="/var/log/$(basename $0 .sh)_$(date +%y%m%d%H%M%S).log"

# Validations
[[ ! -f $LOGFILE ]] && touch $LOGFILE
[[ -z $USER ]] || [[ -z $PASS ]] || [[ -z $DB ]] && usage "ERROR: one or more options (username/password/database) are missing."

# Variables Computed
HOST="${HOST:-$host}"
DB_CONN="mysql -u$USER -p$PASS -h$HOST -D$DB"
[[ ! -z $TABLES ]] && TABLES=$(echo $TABLES | sed 's/,/ /g')

# Main code
vaildate_db
if [[ -z $TABLES ]];then
	tables_list=$(get_tables)
elif [[ ! -z $TABLES ]];then
	tables_list=${TABLES}
fi


for table in $tables_list;do
	log "= = = = = Working on [${DB}.${table}] = = = = ="
	create_new_table
	create_triggers
	continue_check "Failed creating triggers. Drop the table [${DB}.${table}_new] before running the script again." "Created triggers [ OK ]"
	copy_data
	rename_tables
	drop_triggers
	drop_old_table
	[[ $? -eq 0 ]] && log "Successfully Optimized [${DB}.${table}]"
done

log "* * * Please check errors in [$LOGFILE] before running the script again. * * *"
