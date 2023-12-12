#!/bin/sh

######################################################################################################
# CREATION - 01/05/2021 - S40435
# PURPOSE OF THE PROGRAMM :
#
#          Import of a delimited or fixedwidth file into a oracle or vertica table (FULL & UPDT).
#
# VARIABLE(S) :
# 1 : Filename properties
#####################################################################################################

# Var
root_path=/app/list/data
echo "---root_path---"$root_path
VERTICA_TOOLS=/opt/vertica/bin
echo "---VERTICA_TOOLS---"$VERTICA_TOOLS
world=$(echo `id` | cut -d'(' -f 2 | cut -d')' -f 1 | tail -c3)
echo "---world---"$world
version=$(echo `pwd` | cut -d'/' -f14)
echo "---version---"$version
sysdate=$(date +"%Y%m%d")
echo "---sysdate---"$sysdate

## Split the argument to get back all variables
property_name=$1 #source.base.object
property_name=${property_name,,}
echo "---property_name---"$property_name
source=`echo $property_name | awk -F'.' '{ print $1 }'`
echo "---source---"$source
base=`echo $property_name | awk -F'.' '{ print $2 }'`
echo "---base---"$base
object=`echo $property_name | awk -F'.' '{ print $3 }'`
echo "---object---"$object

#Centralized CTX (get MAPR_RootPath, ORACLE_TOOLS, **ORACLE_USER, ***ORACLE_LISTENER)
CtxCentralizedFilePath=${root_path}/factory/${world}/app/common/socdb/configuration/centralized/context/centralizedContext.properties
echo "---CtxCentralizedFilePath---"$CtxCentralizedFilePath
MAPR_RootPath=`grep -w MAPR_RootPath ${CtxCentralizedFilePath} | cut -d= -f2`
echo "---MAPR_RootPath---"$MAPR_RootPath
PwdCentralisedFilePath=${root_path}/factory/${world}/app/common/socdb/configuration/centralized/context
echo "---PwdCentralisedFilePath---"$PwdCentralisedFilePath
data_path=${root_path}${MAPR_RootPath}${world^^}/DATA/RAW/FILE/${source^^}/${base^^}/${object^^}
echo "---data_path---"$data_path
ctrl_path=${root_path}/factory/${world}/app/source/${source}/processing/batch/loader/SOCDB_DataLoader/conf
echo "---ctrl_path---"$ctrl_path
log_path=${root_path}/factory/${world}/log
echo "---log_path---"$log_path
bad_path=${root_path}${MAPR_RootPath}${world^^}/DATA/RAW/FILE/${source^^}/${base^^}/${object^^}/DATALOADER/BAD
echo "---bad_path---"$bad_path
discard_path=${root_path}${MAPR_RootPath}${world^^}/DATA/RAW/FILE/${source^^}/${base^^}/${object^^}/DATALOADER/DISCARD
echo "---discard_path---"$discard_path
log_file=${log_path}/shell/socdb/${sysdate}.SOCDB_DataLoader.${source}.${base}.${object}.log
echo "---log_file---"$log_file
log_file_oracle=${log_path}/oracle/socdb/${sysdate}.SOCDB_DataLoader_oracle.${source}.${base}.${object}.log
echo "---log_file_oracle---"$log_file_oracle
log_badfile_oracle=${log_path}/oracle/socdb/${sysdate}.SOCDB_DataLoader_oracle.${source}.${base}.${object}.bad.log
echo "---log_badfile_oracle---"$log_badfile_oracle
log_file_vertica=${log_path}/vertica/socdb/${sysdate}.SOCDB_DataLoader_vertica.${source}.${base}.${object}.log
echo "---log_file_vertica---"$log_file_vertica
log_badfile_vertica=${log_path}/vertica/socdb/${sysdate}.SOCDB_DataLoader_vertica.${source}.${base}.${object}.bad.log
echo "---log_badfile_vertica---"$log_badfile_vertica

# FUNCTIONS
function func_LOG {
	## if empty, we create it
	if [[ -n "$1" ]] ; then
		LOGFILE=$log_file
	fi
	echo "$@" >> $LOGFILE
}

function func_INFO {
	date_system_timestamp=$(date '+%Y%m%d_%H%M%S')
	echo "[INFO][${date_system_timestamp}] $@"
	func_LOG "[INFO][${date_system_timestamp}] $@"
}

function func_WARN {
	date_system_timestamp=$(date '+%Y%m%d_%H%M%S')
	echo "[WARN][${date_system_timestamp}] $@"
	func_LOG "[WARN][${date_system_timestamp}] $@"
}

function func_ERREUR {
	date_system_timestamp=$(date '+%Y%m%d_%H%M%S')
	echo "[ERROR][${date_system_timestamp}] $@"
	func_LOG "[ERROR][${date_system_timestamp}] $@"
	if [[ -n "$f" ]] ; then
		# Move file to error
		mv "$f" ${data_path}/ERROR/
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to move file $f in ${data_path}/ERROR"
		fi
	fi
	exit 1
}

function func_LoadOracle {
	func_INFO "LoadOracle start."

	### Test temporary table existing
	func_TableExist ${tablename}_DATALOADER
	if [ $TE_count -eq 1 ]; then
		func_INFO "Table ${DATABASE_SCHEMA}.${tablename}_DATALOADER exist. Delete."
		#${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@"(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = ${ORACLE_SERVER})(PORT = ${ORACLE_PORT})) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = ${ORACLE_LISTENER}) (FAILOVER_MODE = (TYPE = select) (METHOD = basic))))" <<EOF>>$log_file
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
DROP TABLE ${DATABASE_SCHEMA}."${tablename}_DATALOADER";
EOF
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to delete ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
		fi
	fi

	### Create temporary table
	if [ -n "${COLUMN_CUSTOM}" ]; then
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER @${ctrl_path}/${property_name}/${source}.${base}.${object}.sql4>>$log_file
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
		fi
	else
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
DECLARE
	sql_stmt clob;
BEGIN
	SELECT REPLACE(DBMS_METADATA.GET_DDL('TABLE','${tablename}'),'${tablename}','${tablename}_DATALOADER') INTO sql_stmt FROM DUAL;
	EXECUTE IMMEDIATE sql_stmt;
END;
/
EOF
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
		fi
	fi
	func_TableExist ${tablename}_DATALOADER
	if [ $TE_count -eq 1 ]; then
		func_INFO "Table ${DATABASE_SCHEMA}.${tablename}_DATALOADER created."
	else
		func_ERREUR "Table ${DATABASE_SCHEMA}.${tablename}_DATALOADER not created."
	fi

	### Grant permission on ${DATABASE_SCHEMA}.${tablename}_DATALOADER
	func_INFO "Grant permission on ${DATABASE_SCHEMA}.${tablename}_DATALOADER to ROLE_${ORACLE_USER}_RO."
	${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
GRANT SELECT ON ${DATABASE_SCHEMA}."${tablename}_DATALOADER" TO ROLE_${ORACLE_USER}_RO;
COMMIT;
EOF
	if [ $? -ne 0 ]; then
		func_WARN "Failing to grant permission on ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
	fi
	
	### Load data into temporary table	
	${ORACLE_HOME}/bin/sqlldr userid=${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER control=${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl log=${log_file_oracle} bad=${bad_path}/${sysdate}_${source}.${base}.${object}.bad discard=${discard_path}/${sysdate}_${source}.${base}.${object}.dis direct=y errors=${THRESHOLD}
		
	LO_retcode=`echo $?`
	case "$LO_retcode" in 
	0) func_INFO "SQL*Loader (code $LO_retcode) - Success to load data from $f." ;;
	1) func_ERREUR "SQL*Loader (code $LO_retcode) - Failing to load data from $f. Failure (OS related errors (command line/syntax errors, oracle errors fatal to SQL*Loader))." ;;
	2) func_WARN "SQL*Loader (code $LO_retcode) - Success to load data from $f with WARNINGS (see logfile)." ;;
	3) func_ERREUR "SQL*Loader (code $LO_retcode) - Failing to load data from $f. Fatal error (OS related errors (like file open/close, etc.))." ;;
	*) func_ERREUR "SQL*Loader (code $LO_retcode) - Failing to load data from $f. Unknown return code." ;;
	esac

	if [ -e "${bad_path}/${sysdate}_${source}.${base}.${object}.bad" ] && [ -s "${bad_path}/${sysdate}_${source}.${base}.${object}.bad" ]; then
		func_WARN "File ${bad_path}/${sysdate}_${source}.${base}.${object}.bad not empty."
		func_WARN "For more details see oracle log ${log_file_oracle}."
		
		# Add STAT_DETAIL
		func_InsertSOCDB_STAT_DETAIL $ID NB_Rows_bad_file `wc -l ${bad_path}/${sysdate}_${source}.${base}.${object}.bad | cut -d' ' -f-1`
		
		### Test temporary table existing
		func_TableExist ${tablename}_REJECTED
		if [ $TE_count -eq 1 ]; then
			func_INFO "Table ${DATABASE_SCHEMA}.${tablename}_REJECTED exist."
		else
			### Create rejected table
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
CREATE TABLE ${DATABASE_SCHEMA}."${tablename}_REJECTED" (
S_ID number (10),
METADATA_TM_INSERT TIMESTAMP (0),
LINE CLOB)
TABLESPACE ${ORACLE_USER^^}DATAG;
EOF
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to create ${DATABASE_SCHEMA}.${tablename}_REJECTED."
			fi
		fi

		### Grant permission on ${DATABASE_SCHEMA}.${tablename}_REJECTED
		func_INFO "Grant permission on ${DATABASE_SCHEMA}.${tablename}_REJECTED to ROLE_${ORACLE_USER}_RO."
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
GRANT SELECT ON ${DATABASE_SCHEMA}."${tablename}_REJECTED" TO ROLE_${ORACLE_USER}_RO;
COMMIT;
EOF
		if [ $? -ne 0 ]; then
			func_WARN "Failing to grant permission on ${DATABASE_SCHEMA}.${tablename}_REJECTED."
		fi
		
		### Replace --ID--
		sed -i "s|--ID--|$ID|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.bad.ctrl
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to replace ID in ${ctrl_path}/${property_name}/${source}.${base}.${object}.bad.ctrl"
		fi
		
		### Load bad data into rejected table	
		${ORACLE_HOME}/bin/sqlldr userid=${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER control=${ctrl_path}/${property_name}/${source}.${base}.${object}.bad.ctrl log=${log_badfile_oracle} bad=${bad_path}/${sysdate}_${source}.${base}.${object}.bad.bad direct=y errors=0
		
		BAD_retcode=`echo $?`
		case "$BAD_retcode" in 
		0) func_INFO "SQL*Loader (code $BAD_retcode) - Success to load data from ${bad_path}/${sysdate}_${source}.${base}.${object}.bad." ;;
		1) func_ERREUR "SQL*Loader (code $BAD_retcode) - Failing to load data from ${bad_path}/${sysdate}_${source}.${base}.${object}.bad. Failure (OS related errors (command line/syntax errors, oracle errors fatal to SQL*Loader))." ;;
		2) func_WARN "SQL*Loader (code $BAD_retcode) - Success to load data from ${bad_path}/${sysdate}_${source}.${base}.${object}.bad with WARNINGS (see logfile)." ;;
		3) func_ERREUR "SQL*Loader (code $BAD_retcode) - Failing to load data from ${bad_path}/${sysdate}_${source}.${base}.${object}.bad. Fatal error (OS related errors (like file open/close, etc.))." ;;
		*) func_ERREUR "SQL*Loader (code $BAD_retcode) - Failing to load data from ${bad_path}/${sysdate}_${source}.${base}.${object}.bad. Unknown return code." ;;
		esac
		
		### Count rows after
		func_NbRows ${DATABASE_SCHEMA} ${tablename}_REJECTED S_ID $ID
		func_INFO "Row number after loading into ${DATABASE_SCHEMA}.${tablename}_REJECTED = $NR_count"
		# Add STAT_DETAIL
		func_InsertSOCDB_STAT_DETAIL $ID NBRows_rejected_table $NR_count

		### Check threshold
		if [ "$NR_count" -ge "${THRESHOLD}" ];then
			# Add STAT_DETAIL
			func_InsertSOCDB_STAT_DETAIL $ID 'ERROR' 'NBRows_rejected too big'
			func_ERREUR "Number of rejected rows > ${THRESHOLD}."
		fi
	fi
	
	if [ -e "${discard_path}/${sysdate}_${source}.${base}.${object}.dis" ] && [ -s "${discard_path}/${sysdate}_${source}.${base}.${object}.dis" ]; then
		func_WARN "File ${discard_path}/${sysdate}_${source}.${base}.${object}.dis not empty."
		
		# Add STAT_DETAIL
		func_InsertSOCDB_STAT_DETAIL $ID NB_Rows_discard_file `wc -l ${discard_path}/${sysdate}_${source}.${base}.${object}.dis | cut -d' ' -f-1`
	fi
	
	### Count rows after
	func_NbRows ${DATABASE_SCHEMA} ${tablename}_DATALOADER 1 1
	func_INFO "Row number after loading into ${DATABASE_SCHEMA}.${tablename}_DATALOADER = $NR_count"
	NR_count_TEMP=$NR_count

	# Add STAT_DETAIL
	func_InsertSOCDB_STAT_DETAIL $ID Loading_temp_table return_code=$LO_retcode

	# Apply Custom queries
	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3" ]; then
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER @${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3>>$log_file
		func_NbRows ${DATABASE_SCHEMA} ${tablename}_DATALOADER 1 1
		func_INFO "Row number after applying custom queries on ${DATABASE_SCHEMA}.${tablename}_DATALOADER = $NR_count"
		NR_count_TEMP=$NR_count
	fi

	# Add STAT_DETAIL
	func_InsertSOCDB_STAT_DETAIL $ID NBRows_temp_table $NR_count
	
	# Check si doublons à la clé de réconciliation
	if [ -n "${COLUMN_ID}" ]; then	
		### Test temporary table existing
		func_TableExist ${tablename}_CTRL
		if [ $TE_count -eq 1 ]; then
			func_INFO "Table ${DATABASE_SCHEMA}.${tablename}_CTRL exist. Delete."
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
DROP TABLE ${DATABASE_SCHEMA}."${tablename}_CTRL";
EOF
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to delete ${DATABASE_SCHEMA}.${tablename}_CTRL."
			fi
		fi
		### Create temporary table
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
CREATE TABLE ${DATABASE_SCHEMA}."${tablename}_CTRL" 
(COUNTKEY number (10))
TABLESPACE ${ORACLE_USER^^}DATAG;
EOF
		if [ $? -ne 0 ]; then
				func_ERREUR "Failing to create ${DATABASE_SCHEMA}.${tablename}_CTRL."
		fi		
		### Check duplicate keys
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER @${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2>>$log_file
		NR_countPk=$(echo "SELECT COUNTKEY FROM ${DATABASE_SCHEMA}."${tablename}_CTRL" where rownum=1;" | ${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER |tail -2 |xargs)
		if [ "${NR_countPk}" != "${NR_count_TEMP}" ]; then
			func_ERREUR "Duplicate keys present into ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
		else
			func_INFO "No duplicate keys present into ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
			### Delete temp table
			func_INFO "Delete table ${DATABASE_SCHEMA}.${tablename}_CTRL."
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
DROP TABLE ${DATABASE_SCHEMA}."${tablename}_CTRL";
EOF
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to delete ${DATABASE_SCHEMA}.${tablename}_CTRL."
			fi
		fi
	fi
	
	if [[ "${loadmode}" == "FULL" ]]; then
		### Truncate table
		func_INFO "Truncate table ${DATABASE_SCHEMA}.${tablename}."
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
TRUNCATE TABLE ${DATABASE_SCHEMA}."${tablename}";
EOF
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to truncate ${DATABASE_SCHEMA}.${tablename}."
		fi
		
		### Count rows before
		func_NbRows ${DATABASE_SCHEMA} ${tablename} 1 1
		func_INFO "Row number before loading into ${DATABASE_SCHEMA}.${tablename} = $NR_count"
		
		# Add STAT_DETAIL
		func_InsertSOCDB_STAT_DETAIL $ID NBRows_table_after_truncate $NR_count
		
		### Insert data from table temp into table
		func_INFO "Insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
		if [ -n "${SELECT_CUSTOM}" ]; then
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
INSERT /*+ APPEND */ INTO ${DATABASE_SCHEMA}."${tablename}" SELECT METADATA_TM_FILE, METADATA_TM_INSERT, ${SELECT_CUSTOM} FROM ${DATABASE_SCHEMA}."${tablename}_DATALOADER";
COMMIT;
EOF
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		else
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
INSERT /*+ APPEND */ INTO ${DATABASE_SCHEMA}."${tablename}" SELECT * FROM ${DATABASE_SCHEMA}."${tablename}_DATALOADER";
COMMIT;
EOF
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		fi

		### Count rows after
		func_NbRows ${DATABASE_SCHEMA} ${tablename} 1 1
		func_INFO "Row number after loading into ${DATABASE_SCHEMA}.${tablename} = $NR_count"
		NR_count_TABLE=$NR_count
		
		### Compare insterted rows
		if [ $NR_count_TABLE -eq $NR_count_TEMP ]; then
			func_INFO "Row inserted into ${DATABASE_SCHEMA}.${tablename} = Row inserted into ${DATABASE_SCHEMA}.${tablename}_DATALOADER"
			# Add STAT_DETAIL
			func_InsertSOCDB_STAT_DETAIL $ID NBRows_table_after_loading=NBRows_temp_table ''
		else
			func_ERREUR "Row inserted into ${DATABASE_SCHEMA}.${tablename} != Row inserted into ${DATABASE_SCHEMA}.${tablename}_DATALOADER"
		fi
	fi
	
	if [[ "${loadmode}" == "UPDT" ]]; then
		### Apply delete request onto table
		func_INFO "Apply delete request onto ${DATABASE_SCHEMA}.${tablename}."
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
${DELETE_REQUEST}
COMMIT;
EOF
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to apply delete request onto ${DATABASE_SCHEMA}.${tablename}."
		fi
		
		### Count rows after
		func_NbRows ${DATABASE_SCHEMA} ${tablename} 1 1
		func_INFO "Row number after apply delete request onto ${DATABASE_SCHEMA}.${tablename} = $NR_count"
		
		# Add STAT_DETAIL
		func_InsertSOCDB_STAT_DETAIL $ID NBRows_table_after_delete_request $NR_count
		
		### Insert data from table temp into table
		func_INFO "Insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
		if [ -n "${SELECT_CUSTOM}" ]; then
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
INSERT /*+ APPEND */ INTO ${DATABASE_SCHEMA}."${tablename}" SELECT METADATA_TM_FILE, METADATA_TM_INSERT, ${SELECT_CUSTOM} FROM ${DATABASE_SCHEMA}."${tablename}_DATALOADER";
COMMIT;
EOF
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		else
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
INSERT /*+ APPEND */ INTO ${DATABASE_SCHEMA}."${tablename}" SELECT * FROM ${DATABASE_SCHEMA}."${tablename}_DATALOADER";
COMMIT;
EOF
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		fi
	fi

	if [[ "${loadmode}" == "INST" ]]; then		
		### Insert data from table temp into table
		func_INFO "Insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
		if [ -n "${SELECT_CUSTOM}" ]; then
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
INSERT /*+ APPEND */ INTO ${DATABASE_SCHEMA}."${tablename}" SELECT METADATA_TM_FILE, METADATA_TM_INSERT, ${SELECT_CUSTOM} FROM ${DATABASE_SCHEMA}."${tablename}_DATALOADER";
COMMIT;
EOF
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		else
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
INSERT /*+ APPEND */ INTO ${DATABASE_SCHEMA}."${tablename}" SELECT * FROM ${DATABASE_SCHEMA}."${tablename}_DATALOADER";
COMMIT;
EOF
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		fi
	fi
	
	### Statistics calculation on ${DATABASE_SCHEMA}.${tablename}
	func_INFO "Statistics calculation on ${DATABASE_SCHEMA}.${tablename}."
	${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
EXEC DBMS_STATS.gather_table_stats('${ORACLE_USER}', '"${tablename}"');
COMMIT;
EOF
	if [ $? -ne 0 ]; then
		func_ERREUR "Failing to calculate statistics on ${DATABASE_SCHEMA}.${tablename}."
	fi
	
	# Add STAT_DETAIL
	func_InsertSOCDB_STAT_DETAIL $ID Statistics_Calculation ''

	### Delete temp table
	func_INFO "Delete table ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
	${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
DROP TABLE ${DATABASE_SCHEMA}."${tablename}_DATALOADER";
EOF
	if [ $? -ne 0 ]; then
		func_ERREUR "Failing to delete ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
	fi
	
	func_INFO "LoadOracle end."
}

function func_LoadVertica {
	func_INFO "LoadVertica start."
	
	### Test temporary table existing
	func_TableExist ${tablename}_DATALOADER
	if [ $TE_count -eq 1 ]; then
		func_INFO "Table ${DATABASE_SCHEMA}.${tablename}_DATALOADER exist. Delete."
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"DROP TABLE IF EXISTS ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" CASCADE;">> $log_file 2>&1
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to delete ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
		fi
	fi
	
	### Create temporary table
	if [ -n "${COLUMN_CUSTOM}" ]; then
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -f ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3>> $log_file 2>&1
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
		fi
	else
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"CREATE TABLE ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" LIKE ${DATABASE_SCHEMA}."${tablename}" EXCLUDING PROJECTIONS;
		ALTER TABLE ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" DROP COLUMN METADATA_TM_FILE CASCADE;
		ALTER TABLE ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" DROP COLUMN METADATA_TM_INSERT CASCADE;">> $log_file 2>&1	
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
		fi
	fi
	
	func_TableExist ${tablename}_DATALOADER
	if [ $TE_count -eq 1 ]; then
		func_INFO "Table ${DATABASE_SCHEMA}.${tablename}_DATALOADER created."
	else
		func_ERREUR "Table ${DATABASE_SCHEMA}.${tablename}_DATALOADER not created."
	fi



	# Check Delimited or Positionned file
	if [ -n "${DELIMITER}" ] ; then
		func_INFO "DELIMITER = ${DELIMITER}."
	else 
		func_INFO "DELIMITER is empty. Fixed-width data file."
		if [ -z "${FIXEDWIDTH}" ] ; then
			func_ERREUR "FIXEDWIDTH parameter is empty."
		fi
	fi

	# Initialize SKIP
	if [ -n "${SKIP}" ] ; then
		func_INFO "SKIP = ${SKIP}. Process will ignore the ${SKIP} first lines."
	else 
		SKIP=0
		func_INFO "SKIP = default value (0)."
	fi
	
	# Initialize THRESHOLD
	if [ -n "${THRESHOLD}" ] ; then
		func_INFO "THRESHOLD = ${THRESHOLD}. Process will stop if you have more than ${THRESHOLD} reject."
	else 
		THRESHOLD=1000000
		func_INFO "THRESHOLD = default value (1000000)."
	fi
	
	### Load data into temporary table
	if [ -n "${DELIMITER}" ] ; then
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"COPY ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" FROM LOCAL '$f' DELIMITER E'${DELIMITER}' NULL '' SKIP ${SKIP} REJECTMAX ${THRESHOLD} DIRECT NO ESCAPE REJECTED DATA '${bad_path}/${sysdate}_${source}.${base}.${object}.bad' EXCEPTIONS '${log_file_vertica}';"  >> $log_file 2>&1
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to copy data FROM ${f} TO ${DATABASE_SCHEMA}.${tablename}_DATALOADER"
		fi
	elif [ -z "${DELIMITER}" ] && [ -n "${FIXEDWIDTH}" ] ; then
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"COPY ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" FROM LOCAL '$f' FIXEDWIDTH colsizes (${FIXEDWIDTH}) NULL ' ' SKIP ${SKIP} REJECTMAX ${THRESHOLD} DIRECT REJECTED DATA '${bad_path}/${sysdate}_${source}.${base}.${object}.bad' EXCEPTIONS '${log_file_vertica}';"  >> $log_file 2>&1
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to copy data FROM ${f} TO ${DATABASE_SCHEMA}.${tablename}_DATALOADER"
		fi
	else
		func_ERREUR "DELIMITER parameter is empty and FIXEDWIDTH empty too."
	fi
	
	if [ -e "${bad_path}/${sysdate}_${source}.${base}.${object}.bad" ] && [ -s "${bad_path}/${sysdate}_${source}.${base}.${object}.bad" ]; then
		func_WARN "File ${bad_path}/${sysdate}_${source}.${base}.${object}.bad not empty."
		func_WARN "For more details see vertica log ${log_file_vertica}."
		
		# Add STAT_DETAIL
		func_InsertSOCDB_STAT_DETAIL $ID NB_Rows_bad_file `wc -l ${bad_path}/${sysdate}_${source}.${base}.${object}.bad | cut -d' ' -f-1`
		
		### Test temporary table existing
		func_TableExist ${tablename}_REJECTED
		if [ $TE_count -eq 1 ]; then
			func_INFO "Table ${DATABASE_SCHEMA}.${tablename}_REJECTED exist."
		else
			### Create rejected table
			${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
			"CREATE TABLE ${DATABASE_SCHEMA}.\"${tablename}_REJECTED\" (
			S_ID number (10),
			METADATA_TM_INSERT TIMESTAMP (0),
			LINE LONG VARCHAR);">> $log_file 2>&1	
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to create ${DATABASE_SCHEMA}.${tablename}_REJECTED."
			fi
		fi
		
		### Load bad data into rejected table
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"COPY ${DATABASE_SCHEMA}.\"${tablename}_REJECTED\" (S_ID as $ID, METADATA_TM_INSERT as sysdate(), LINE) FROM LOCAL '${bad_path}/${sysdate}_${source}.${base}.${object}.bad' DELIMITER U&'\0006' NULL '' DIRECT NO ESCAPE REJECTED DATA '${bad_path}/${sysdate}_${source}.${base}.${object}.bad.bad' EXCEPTIONS '${log_badfile_vertica}' abort on error;"  >> $log_file 2>&1
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to copy data FROM ${f} TO ${DATABASE_SCHEMA}.${tablename}_REJECTED"
		fi
		
		### Count rows after
		func_NbRows ${DATABASE_SCHEMA} ${tablename}_REJECTED S_ID $ID
		func_INFO "Row number after loading into ${DATABASE_SCHEMA}.${tablename}_REJECTED = $NR_count"
		# Add STAT_DETAIL
		func_InsertSOCDB_STAT_DETAIL $ID NBRows_rejected_table $NR_count

		### Check threshold
		if [ "$NR_count" -ge "${THRESHOLD}" ];then
			# Add STAT_DETAIL
			func_InsertSOCDB_STAT_DETAIL $ID 'ERROR' 'NBRows_rejected too big'
			func_ERREUR "Number of rejected rows > ${THRESHOLD}."
		fi
	fi
	
	### Count rows after
	func_NbRows ${DATABASE_SCHEMA} ${tablename}_DATALOADER 1 1
	func_INFO "Row number after loading into ${DATABASE_SCHEMA}.${tablename}_DATALOADER = $NR_count"
	NR_count_TEMP=$NR_count
	
	# Apply Custom queries
	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2" ]; then
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -f ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2>> $log_file 2>&1
		func_NbRows ${DATABASE_SCHEMA} ${tablename}_DATALOADER 1 1
		func_INFO "Row number after applying custom queries on ${DATABASE_SCHEMA}.${tablename}_DATALOADER = $NR_count"
		NR_count_TEMP=$NR_count
	fi

	# Add STAT_DETAIL
	func_InsertSOCDB_STAT_DETAIL $ID NBRows_temp_table $NR_count

	# Check si doublons à la clé de réconciliation
	if [ -n "${COLUMN_ID}" ]; then
		NR_countPK=$(${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c "SELECT count(0) FROM (SELECT distinct ${COLUMN_ID} FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\")subquery;")
		NR_countPK=$(($(echo $NR_countPK | xargs) + 0))
		if [ "${NR_countPK}" != "${NR_count}" ]; then
			func_ERREUR "Duplicate keys present into ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
		else
			func_INFO "No duplicate keys present into ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
		fi
	fi

	if [[ "${loadmode}" == "FULL" ]]; then
		### Truncate table
		func_INFO "Truncate table ${DATABASE_SCHEMA}.${tablename}."
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"TRUNCATE TABLE ${DATABASE_SCHEMA}.\"${tablename}\";">> $log_file 2>&1
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to truncate ${DATABASE_SCHEMA}.${tablename}."
		fi
		
		### Count rows before
		func_NbRows ${DATABASE_SCHEMA} ${tablename} 1 1
		func_INFO "Row number before loading into ${DATABASE_SCHEMA}.${tablename} = $NR_count"
		
		# Add STAT_DETAIL
		func_InsertSOCDB_STAT_DETAIL $ID NBRows_table_after_truncate $NR_count
		
		### Insert data from table temp into table
		func_INFO "Insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
		if [ -n "${SELECT_CUSTOM}" ]; then
			${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
			"INSERT /*+ DIRECT */ INTO ${DATABASE_SCHEMA}.\"${tablename}\" SELECT TO_DATE('$filedate','YYYYMMDD-HHMISS'),sysdate(),${SELECT_CUSTOM} FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\";COMMIT;">> $log_file 2>&1
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		else
			${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
			"INSERT /*+ DIRECT */ INTO ${DATABASE_SCHEMA}.\"${tablename}\" SELECT TO_DATE('$filedate','YYYYMMDD-HHMISS'),sysdate(),* FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\";COMMIT;">> $log_file 2>&1
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		fi
		
		### Count rows after
		func_NbRows ${DATABASE_SCHEMA} ${tablename} 1 1
		func_INFO "Row number after loading into ${DATABASE_SCHEMA}.${tablename} = $NR_count"
		NR_count_TABLE=$NR_count
		
		### Compare insterted rows
		if [ $NR_count_TABLE -eq $NR_count_TEMP ]; then
			func_INFO "Row inserted into ${DATABASE_SCHEMA}.${tablename} = Row inserted into ${DATABASE_SCHEMA}.${tablename}_DATALOADER"
			# Add STAT_DETAIL
			func_InsertSOCDB_STAT_DETAIL $ID NBRows_table_after_loading=NBRows_temp_table ''
		else
			func_ERREUR "Row inserted into ${DATABASE_SCHEMA}.${tablename} != Row inserted into ${DATABASE_SCHEMA}.${tablename}_DATALOADER"
		fi
	fi

if [[ "${loadmode}" == "UPDT" ]]; then
		### Apply delete request onto table
		func_INFO "Apply delete request onto ${DATABASE_SCHEMA}.${tablename}."
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"${DELETE_REQUEST}COMMIT;">> $log_file 2>&1
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to apply delete request onto ${DATABASE_SCHEMA}.${tablename}."
		fi
		
		### Count rows after
		func_NbRows ${DATABASE_SCHEMA} ${tablename} 1 1
		func_INFO "Row number after apply delete request onto ${DATABASE_SCHEMA}.${tablename} = $NR_count"
		
		# Add STAT_DETAIL
		func_InsertSOCDB_STAT_DETAIL $ID NBRows_table_after_delete_request $NR_count

		### Insert data from table temp into table
		func_INFO "Insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
		if [ -n "${SELECT_CUSTOM}" ]; then
			${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
			"INSERT /*+ DIRECT */ INTO ${DATABASE_SCHEMA}.\"${tablename}\" SELECT TO_DATE('$filedate','YYYYMMDD-HHMISS'),sysdate(),${SELECT_CUSTOM} FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\";COMMIT;">> $log_file 2>&1
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		else
			${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
			"INSERT /*+ DIRECT */ INTO ${DATABASE_SCHEMA}.\"${tablename}\" SELECT TO_DATE('$filedate','YYYYMMDD-HHMISS'),sysdate(),* FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\";COMMIT;">> $log_file 2>&1
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		fi		
	fi
	
	if [[ "${loadmode}" == "INST" ]]; then		
		### Insert data from table temp into table
		func_INFO "Insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
		if [ -n "${SELECT_CUSTOM}" ]; then
			${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
			"INSERT /*+ DIRECT */ INTO ${DATABASE_SCHEMA}.\"${tablename}\" SELECT TO_DATE('$filedate','YYYYMMDD-HHMISS'),sysdate(),${SELECT_CUSTOM} FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\";COMMIT;">> $log_file 2>&1
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		else
			${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
			"INSERT /*+ DIRECT */ INTO ${DATABASE_SCHEMA}.\"${tablename}\" SELECT TO_DATE('$filedate','YYYYMMDD-HHMISS'),sysdate(),* FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\";COMMIT;">> $log_file 2>&1
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to insert data from ${DATABASE_SCHEMA}.${tablename}_DATALOADER into ${DATABASE_SCHEMA}.${tablename}."
			fi
		fi		
	fi
	
	### Statistics calculation on ${DATABASE_SCHEMA}.${tablename}
	func_INFO "Statistics calculation on ${DATABASE_SCHEMA}.${tablename}."
	${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
	"SELECT ANALYZE_STATISTICS ('${DATABASE_SCHEMA}.\"${tablename}\"');">> $log_file 2>&1
	if [ $? -ne 0 ]; then
		func_ERREUR "Failing to calculate statistics on ${DATABASE_SCHEMA}.${tablename}."
	fi
	
	# Add STAT_DETAIL
	func_InsertSOCDB_STAT_DETAIL $ID Statistics_Calculation ''
	
	### Delete temp table
	func_INFO "Delete table ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
	${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
	"DROP TABLE IF EXISTS ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" CASCADE;">> $log_file 2>&1
	if [ $? -ne 0 ]; then
		func_ERREUR "Failing to delete ${DATABASE_SCHEMA}.${tablename}_DATALOADER."
	fi
	
	func_INFO "LoadVertica end."
}

function func_GenerateOracleFiles {
	func_INFO "GenerateOracleFiles start."
	GOF_array=()
	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl" ]; then
		rm ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl
	fi
	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql" ]; then
		rm ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
	fi
	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2" ]; then
		rm ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2
	fi
	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3" ]; then
		rm ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3
	fi
	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql4" ]; then
		rm ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql4
	fi
	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.bad.ctrl" ]; then
		rm ${ctrl_path}/${property_name}/${source}.${base}.${object}.bad.ctrl
	fi
	if [ -f "${ctrl_path}/${property_name}/tnsnames.ora" ]; then
		rm ${ctrl_path}/${property_name}/tnsnames.ora
	fi
	while IFS= read -r GOF_line; do
		[[ $GOF_line == END_OPTION=END_OPTION ]] && GOF_printline="no"
		[[ $GOF_printline == "yes" ]] && GOF_array+=("$GOF_line") GOF_var_option=$GOF_var_option,$GOF_line
		[[ $GOF_line == START_OPTION=START_OPTION ]] && GOF_printline="yes"
	done < "${ctrl_path}/${source}.${base}.${object}.properties"
	
	GOF_option=${GOF_var_option:1:${#GOF_var_option}}
	
	if [ ! -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl" ] && [ ! -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql" ] && [ ! -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.bad.ctrl" ]; then
		IFS='|'
		##.sql file
		read -ra GOF_addr <<< "$VARIABLE_NAME_TYPE"
		##Initialize the table with metadatas 
		printf  %"s\n" "create table ${DATABASE_SCHEMA}.\"${tablename}\" (METADATA_TM_FILE TIMESTAMP (0),METADATA_TM_INSERT TIMESTAMP (0)" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql"
		else 
			func_INFO "File ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql created."
		fi
		for i in "${GOF_addr[@]}"; do
			#GOF_var="$(cut -d' ' -f1<<<"$i")"
			#GOF_varDelimitedcoma=$GOF_varDelimitedcoma,$i
			#GOF_varDelimiteds="${GOF_varDelimiteds},${GOF_var}"
			printf %"s\n" ",$i" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
		done
		printf  %"s\n" ") TABLESPACE ${ORACLE_USER^^}DATAG;" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
		##.ctrl file
		#GOF_fields=${GOF_varDelimiteds:1:${#GOF_varDelimiteds}}
		read -ra GOF_addr <<< "$SQLLDR_VARIABLE_NAME"
		for i in "${GOF_addr[@]}"; do
			GOF_varDelimitedcoma=$GOF_varDelimitedcoma,$i
		done
		
		if [ -n "${DELIMITER}" ] ; then
			func_INFO "DELIMITER not empty = Delimited file."
			##delimited
			printf %"s\n"  "OPTIONS ($GOF_option)  " "LOAD DATA CHARACTERSET AL32UTF8 INFILE '--FILE_PARAMETER--'" "--LOAD_MODE--"  "INTO TABLE ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\"" "REENABLE" "FIELDS TERMINATED BY '$DELIMITER'" "TRAILING NULLCOLS" "(METADATA_TM_FILE EXPRESSION \"to_timestamp('--FILE_DATE--','YYYY-MM-DD HH24:MI:SS')\",METADATA_TM_INSERT EXPRESSION \"to_timestamp('"$(date '+%Y-%m-%d %H:%M:%S')"','YYYY-MM-DD HH24:MI:SS')\"$GOF_varDelimitedcoma)" >>${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl"
			else 
				func_INFO "File ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl created."
			fi
		else
			func_INFO "DELIMITER empty = Fixed-width file."
			#fixed-width
			printf %"s\n"  "OPTIONS ($GOF_option)  " "LOAD DATA CHARACTERSET AL32UTF8 LENGTH CHAR INFILE '--FILE_PARAMETER--'" "--LOAD_MODE--"  "INTO TABLE ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\"" "REENABLE" "TRAILING NULLCOLS" "(METADATA_TM_FILE EXPRESSION \"to_timestamp('--FILE_DATE--','YYYY-MM-DD HH24:MI:SS')\",METADATA_TM_INSERT EXPRESSION \"to_timestamp('"$(date '+%Y-%m-%d %H:%M:%S')"','YYYY-MM-DD HH24:MI:SS')\"$GOF_varDelimitedcoma)" >>${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl"
			else 
				func_INFO "File ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl created."
			fi
		fi
		
		printf %"s\n"  "LOAD DATA CHARACTERSET AL32UTF8 INFILE '${bad_path}/${sysdate}_${source}.${base}.${object}.bad'" "APPEND"  "INTO TABLE ${DATABASE_SCHEMA}.\"${tablename}_REJECTED\"" "FIELDS TERMINATED BY '@@${sysdate}@@'"  "(S_ID constant --ID--,METADATA_TM_INSERT EXPRESSION \"to_timestamp('"$(date '+%Y-%m-%d %H:%M:%S')"','YYYY-MM-DD HH24:MI:SS')\",LINE CHAR(40000))" >>${ctrl_path}/${property_name}/${source}.${base}.${object}.bad.ctrl
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/${source}.${base}.${object}.bad.ctrl"
		else 
			func_INFO "File ${ctrl_path}/${property_name}/${source}.${base}.${object}.bad.ctrl created."
		fi
	else
		func_ERREUR "Files ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl & ${ctrl_path}/${property_name}/${source}.${base}.${object}.bad.ctrl & ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql already exist !"
	fi
	
	if [ ! -f "${ctrl_path}/${property_name}/tnsnames.ora" ]; then
	#printf %"s\n"  "${ORACLE_USER^^}_TNS_DATALOADER=(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = ${ORACLE_SERVER})(PORT = ${ORACLE_PORT})) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = ${ORACLE_LISTENER}) (FAILOVER_MODE = (TYPE = select) (METHOD = basic))))" >>${ctrl_path}/${property_name}/tnsnames.ora
	printf %"s\n"  "${ORACLE_USER^^}_TNS_DATALOADER=(DESCRIPTION=(ADDRESS_LIST=(LOAD_BALANCE=off)(FAILOVER=on)(ADDRESS=(PROTOCOL=tcp)(HOST=${ORACLE_SERVERA})(PORT=${ORACLE_PORTA}))(ADDRESS=(PROTOCOL=tcp)(HOST=${ORACLE_SERVERB})(PORT=${ORACLE_PORTB})))(CONNECT_DATA=(SERVICE_NAME=${ORACLE_LISTENER})))" >>${ctrl_path}/${property_name}/tnsnames.ora	
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/tnsnames.ora"
		else 
			func_INFO "File ${ctrl_path}/${property_name}/tnsnames.ora created."
		fi
	else
		func_ERREUR "Files ${ctrl_path}/${property_name}/tnsnames.ora already exist !"
	fi
	# Ajout Pk si nécéssaire + exit 
	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql" ]; then
		# Check key table
		if [ -n "${TABLE_KEY}" ]; then
			printf  %"s\n" "alter table ${DATABASE_SCHEMA}.\"${tablename}\" add (constraint \"${DATABASE_SCHEMA}_${tablename}_PK\" PRIMARY KEY (${TABLE_KEY}));" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
		fi
		printf  %"s\n" "exit" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
	fi
	# Check COLUMN_ID 
	if [ ! -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2" ]; then
		# Check key table
		if [ -n "${COLUMN_ID}" ]; then
			printf  %"s\n" "INSERT INTO ${DATABASE_SCHEMA}.\"${tablename}_CTRL\" SELECT COUNT(0) FROM (SELECT distinct ${COLUMN_ID} FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\")subquery;" "exit" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2"
			else 
				func_INFO "File ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2 created."
			fi
			### Replace , par ,\r\n for line split (sqlplus limitation)
			sed -i "s|,|,\r\n|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to replace , in ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2"
			fi
		fi
	fi
	
	if [ ! -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3" ]; then
		unset GOF_printline
		while IFS= read -r GOF_line; do
			if [[ $GOF_line == END_CUSTOM=END_CUSTOM ]]; then
				GOF_printline="no"
			fi
			if [[ $GOF_printline == "yes" ]]; then
				#:9 delete 9 first characters, :-1 delete the last character
				printf  %"s\n" "${GOF_line:9:-1}" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3
				if [ $? -ne 0 ]; then
					func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3"
				fi
			fi
			if [[ $GOF_line == START_CUSTOM=START_CUSTOM ]]; then
				GOF_printline="yes"
			fi
		done < "${ctrl_path}/${source}.${base}.${object}.properties"

		if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3" ];	then
			func_INFO "File ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3 created."	
			### Replace [SCHEMA] by SCHEMA value
			sed -i "s|\[SCHEMA\]|"${DATABASE_SCHEMA}"|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to replace [SCHEMA] in ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3"
			fi
			### Replace [TABLE] by TABLE value
			sed -i "s|\[TABLE\]|"${tablename}_DATALOADER"|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to replace [TABLE] in ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3"
			fi
		fi
	fi

	if [ ! -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql4" ]; then
		if [ -n "${COLUMN_CUSTOM}" ]; then
			IFS='|'
			##.sql file
			read -ra GOF_addr <<< "$COLUMN_CUSTOM"
			##Initialize the table 
			printf  %"s\n" "create table ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" (METADATA_TM_FILE TIMESTAMP (0) DEFAULT sysdate,METADATA_TM_INSERT TIMESTAMP (0) DEFAULT sysdate" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql4
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql4"
			else 
				func_INFO "File ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql4 created."
			fi
			for i in "${GOF_addr[@]}"; do
				#GOF_var="$(cut -d' ' -f1<<<"$i")"
				#GOF_varDelimitedcoma=$GOF_varDelimitedcoma,$i
				#GOF_varDelimiteds="${GOF_varDelimiteds},${GOF_var}"
				printf %"s\n" ",$i" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql4
			done
			printf  %"s\n" ");" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql4
		fi
	fi
	
	func_INFO "GenerateOracleFiles end."
}

function func_GenerateVerticaFiles {
	func_INFO "GenerateVerticaFiles start."
	GOF_array=()
	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql" ]; then
		rm ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
	fi

	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2" ]; then
		rm ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2
	fi

	if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3" ]; then
		rm ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3
	fi

	if [ ! -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql" ]; then
		IFS='|'
		##.sql file
		read -ra GOF_addr <<< "$VARIABLE_NAME_TYPE"
		##Initialize the table with metadatas 
		printf  %"s\n" "create table ${DATABASE_SCHEMA}.\"${tablename}\" (METADATA_TM_FILE TIMESTAMP (0) DEFAULT sysdate,METADATA_TM_INSERT TIMESTAMP (0) DEFAULT sysdate" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql"
		else 
			func_INFO "File ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql created."
		fi
		for i in "${GOF_addr[@]}"; do
			#GOF_var="$(cut -d' ' -f1<<<"$i")"
			#GOF_varDelimitedcoma=$GOF_varDelimitedcoma,$i
			#GOF_varDelimiteds="${GOF_varDelimiteds},${GOF_var}"
			printf %"s\n" ",$i" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
		done
		printf  %"s\n" ");" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
		# Ajout Pk si ncssaire + exit 
		if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql" ]; then
			# Check key table
			if [ -n "${TABLE_KEY}" ]; then
				printf  %"s\n" "alter table ${DATABASE_SCHEMA}.\"${tablename}\" add constraint \"${DATABASE_SCHEMA}_${tablename}_PK\" PRIMARY KEY (${TABLE_KEY}) enabled;" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
			fi
			printf  %"s\n">> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql
		fi
	fi

	if [ ! -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2" ]; then
		unset GOF_printline
		while IFS= read -r GOF_line; do
			if [[ $GOF_line == END_CUSTOM=END_CUSTOM ]]; then
				GOF_printline="no"
			fi
			if [[ $GOF_printline == "yes" ]]; then
				#:9 delete 9 first characters, :-1 delete the last character
				printf  %"s\n" "${GOF_line:9:-1}" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2
				if [ $? -ne 0 ]; then
					func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2"
				fi
			fi
			if [[ $GOF_line == START_CUSTOM=START_CUSTOM ]]; then
				GOF_printline="yes"
			fi
		done < "${ctrl_path}/${source}.${base}.${object}.properties"

		if [ -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2" ];	then
			func_INFO "File ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2 created."	
			### Replace [SCHEMA] by SCHEMA value
			sed -i "s|\[SCHEMA\]|"${DATABASE_SCHEMA}"|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to replace [SCHEMA] in ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2"
			fi
			### Replace [TABLE] by TABLE value
			sed -i "s|\[TABLE\]|"${tablename}_DATALOADER"|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to replace [TABLE] in ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql2"
			fi
		fi
	fi

	if [ ! -f "${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3" ]; then
		if [ -n "${COLUMN_CUSTOM}" ]; then
			IFS='|'
			##.sql file
			read -ra GOF_addr <<< "$COLUMN_CUSTOM"
			##Initialize the table 
			printf  %"s\n" "create table ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" (METADATA_TM_FILE TIMESTAMP (0) DEFAULT sysdate,METADATA_TM_INSERT TIMESTAMP (0) DEFAULT sysdate" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to create file ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3"
			else 
				func_INFO "File ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3 created."
			fi
			for i in "${GOF_addr[@]}"; do
				#GOF_var="$(cut -d' ' -f1<<<"$i")"
				#GOF_varDelimitedcoma=$GOF_varDelimitedcoma,$i
				#GOF_varDelimiteds="${GOF_varDelimiteds},${GOF_var}"
				printf %"s\n" ",$i" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3
			done
			printf  %"s\n" ");" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3
			printf  %"s\n" "ALTER TABLE ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" DROP COLUMN METADATA_TM_FILE CASCADE;" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3
			printf  %"s\n" "ALTER TABLE ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\" DROP COLUMN METADATA_TM_INSERT CASCADE;" >> ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql3
		fi
	fi

	func_INFO "GenerateVerticaFiles end."
}

function func_TableExist {
	if [[ "${DATABASE}" == "ORACLE" ]]; then
		TE_count=$(echo "SELECT count(0) FROM USER_TABLES WHERE TABLE_NAME=upper('$1');" | ${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER |tail -2 |xargs)
		#TE_count=$(((1*$TE_count)))
	fi
	if [[ "${DATABASE}" == "VERTICA" ]]; then
		TE_count=$(${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c "SELECT count(0) FROM  ALL_TABLES WHERE TABLE_NAME ILIKE upper('$1') and SCHEMA_NAME ILIKE upper('${DATABASE_SCHEMA}');")
		TE_count=$(((1*$TE_count)))
	fi	
	return $TE_count
}

function func_NbRows {
	if [[ "${DATABASE}" == "ORACLE" ]]; then
		NR_count=$(echo "SELECT count(0) FROM $1.\"$2\" WHERE $3=$4;" | ${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER |tail -2 |xargs)
		#NR_count=$(((1*$NR_count)))
	fi
	if [[ "${DATABASE}" == "VERTICA" ]]; then
		NR_count=$(${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c "SELECT count(0) FROM $1.\"$2\" WHERE $3=$4;")
		NR_count=$(((1*$NR_count)))
	fi
	return $NR_count
}

function func_InitializeSOCDB_STAT {
	func_INFO "InitializeSOCDB_STAT start."
	if [[ "${DATABASE}" == "ORACLE" ]]; then
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1

CREATE TABLE ${DATABASE_SCHEMA}.SOCDB_STAT(
  S_ID NUMBER(10) NOT NULL,
  S_PROCESS VARCHAR(200),
  S_PROCESS_VALUE VARCHAR(200),
  S_STATUS INT DEFAULT 0,
  S_INSDATE TIMESTAMP (0) DEFAULT CURRENT_TIMESTAMP,
  S_UPDDATE TIMESTAMP (0) DEFAULT NULL
  ) TABLESPACE ${ORACLE_USER^^}DATAG;

ALTER TABLE ${DATABASE_SCHEMA}.SOCDB_STAT ADD (CONSTRAINT SOCDB_STAT_PK PRIMARY KEY (S_ID));

CREATE SEQUENCE SOCDB_STAT_SQ START WITH 1 NOCACHE;

CREATE OR REPLACE TRIGGER SOCDB_STAT_TR 
BEFORE INSERT ON ${DATABASE_SCHEMA}.SOCDB_STAT 
FOR EACH ROW
BEGIN
	:NEW.S_ID :=SOCDB_STAT_SQ.NEXTVAL;
END;
/
COMMIT;
EOF
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to initialize traces table ${DATABASE_SCHEMA}.SOCDB_STAT."
		else 
			func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT initialized."
		fi
	fi
	if [[ "${DATABASE}" == "VERTICA" ]]; then
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"CREATE TABLE ${DATABASE_SCHEMA}.SOCDB_STAT(
		S_ID IDENTITY(1,1,0),
		S_PROCESS VARCHAR(200),
		S_PROCESS_VALUE VARCHAR(200),
		S_STATUS INT DEFAULT 0,
		S_INSDATE TIMESTAMP (0) DEFAULT sysdate,
		S_UPDDATE TIMESTAMP (0) DEFAULT NULL
		);">> $log_file 2>&1
	fi
	func_INFO "InitializeSOCDB_STAT end."
}

function func_InitializeSOCDB_STAT_DETAIL {
	func_INFO "InitializeSOCDB_STAT_DETAIL start."
	if [[ "${DATABASE}" == "ORACLE" ]]; then
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1

CREATE TABLE ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL(
  SD_ID NUMBER(10) NOT NULL,
  S_ID NUMBER(10) NOT NULL,
  SD_EVENT VARCHAR(200),
  SD_EVENT_VALUE VARCHAR(200),
  SD_INSDATE TIMESTAMP (0) DEFAULT CURRENT_TIMESTAMP
  ) TABLESPACE ${ORACLE_USER^^}DATAG;

ALTER TABLE ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL ADD (CONSTRAINT SOCDB_STAT_DETAIL_PK PRIMARY KEY (SD_ID));

CREATE SEQUENCE SOCDB_STAT_DETAIL_SQ START WITH 1 NOCACHE;

CREATE OR REPLACE TRIGGER SOCDB_STAT_DETAIL_TR 
BEFORE INSERT ON ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL 
FOR EACH ROW
BEGIN
    :NEW.SD_ID :=SOCDB_STAT_DETAIL_SQ.NEXTVAL;
END;
/
COMMIT;
EOF
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to initialize traces table ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL."
		else 
			func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL initialized."
		fi
	fi
	if [[ "${DATABASE}" == "VERTICA" ]]; then
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"CREATE TABLE ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL(
		SD_ID IDENTITY(1,1,0),
		S_ID NUMBER(10) NOT NULL,
		SD_EVENT VARCHAR(200),
		SD_EVENT_VALUE VARCHAR(200),
		SD_INSDATE TIMESTAMP (0) DEFAULT sysdate
		);">> $log_file 2>&1
	fi	
	func_INFO "InitializeSOCDB_STAT_DETAIL end."
}


function func_InsertSOCDB_STAT {
	if [[ "${DATABASE}" == "ORACLE" ]]; then
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
INSERT INTO ${DATABASE_SCHEMA}.SOCDB_STAT (S_PROCESS,S_PROCESS_VALUE) VALUES ('$1','$2');
COMMIT;
EOF
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to insert traces into ${DATABASE_SCHEMA}.SOCDB_STAT."
		else
			func_INFO "Add SOCDB_STAT : $1,$2"
		fi
	fi
	if [[ "${DATABASE}" == "VERTICA" ]]; then
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"INSERT INTO ${DATABASE_SCHEMA}.SOCDB_STAT (S_PROCESS,S_PROCESS_VALUE) VALUES ('$1','$2');COMMIT;">> $log_file 2>&1
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to insert traces into ${DATABASE_SCHEMA}.SOCDB_STAT."
		else
			func_INFO "Add SOCDB_STAT : $1,$2"
		fi
	fi
}

function func_UpdateSOCDB_STAT {
	if [[ "${DATABASE}" == "ORACLE" ]]; then
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
UPDATE ${DATABASE_SCHEMA}.SOCDB_STAT
SET S_STATUS = $2,
S_UPDDATE = (SELECT CURRENT_TIMESTAMP FROM DUAL)
WHERE S_ID = $1;
COMMIT;
EOF
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to update traces into ${DATABASE_SCHEMA}.SOCDB_STAT."
		else
			func_INFO "Update SOCDB_STAT : $1,$2"
		fi
	fi
	if [[ "${DATABASE}" == "VERTICA" ]]; then
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"UPDATE ${DATABASE_SCHEMA}.SOCDB_STAT 
		SET S_STATUS = $2
		,S_UPDDATE = sysdate
		WHERE S_ID = $1;
		COMMIT;">> $log_file 2>&1
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to insert traces into ${DATABASE_SCHEMA}.SOCDB_STAT."
		else
			func_INFO "Add SOCDB_STAT : $1,$2"
		fi
	fi
		
}

function func_InsertSOCDB_STAT_DETAIL {
	if [[ "${DATABASE}" == "ORACLE" ]]; then
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
INSERT INTO ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL (S_ID,SD_EVENT,SD_EVENT_VALUE) VALUES ($1,'$2','$3');
COMMIT;
EOF
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to insert traces into ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL."
		else
			func_INFO "Add SOCDB_STAT_DETAIL : $1,$2;$3"
		fi
	fi
	if [[ "${DATABASE}" == "VERTICA" ]]; then
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c \
		"INSERT INTO ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL (S_ID,SD_EVENT,SD_EVENT_VALUE) VALUES ($1,'$2','$3');COMMIT;">> $log_file 2>&1
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to insert traces into ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL."
		else
			func_INFO "Add SOCDB_STAT_DETAIL : $1,$2;$3"
		fi
	fi
}

function func_GetS_ID {
	if [[ "${DATABASE}" == "ORACLE" ]]; then
		MAX_ID=$(echo "SELECT MAX(S_ID) MAX_ID FROM ${DATABASE_SCHEMA}.SOCDB_STAT WHERE S_PROCESS ='$1' AND S_PROCESS_VALUE='$2';" | ${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER |tail -2 |xargs)
		#MAX_ID=$(((1*$MAX_ID)))
	fi
	if [[ "${DATABASE}" == "VERTICA" ]]; then
		MAX_ID=$(${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -t -c "SELECT MAX(S_ID) MAX_ID FROM ${DATABASE_SCHEMA}.SOCDB_STAT WHERE S_PROCESS ='$1' AND S_PROCESS_VALUE='$2';")
		MAX_ID=$(((1*$MAX_ID)))	
	fi
	return $MAX_ID
}

# PROGRAMM
## Check parameter
if [ "$#" -ne 1 ]; then
	echo "Wrong number of parameters !"
	exit 1
fi

## Creation of logs directory
if [ ! -d "${log_path}" ] ; then
	mkdir ${log_path}
	if [ $? -ne 0 ]; then
		echo "Failing to create directory ${log_path}"
		exit 1
	fi
fi
if [ ! -d "${log_path}/oracle/socdb/" ] ; then
	mkdir -p ${log_path}/oracle/socdb/
	if [ $? -ne 0 ]; then
		echo "Failing to create directory ${log_path}/oracle/socdb/"
		exit 1
	fi
fi
if [ ! -d "${log_path}/shell/socdb/" ] ; then
	mkdir -p ${log_path}/shell/socdb/
	if [ $? -ne 0 ]; then
		echo "Failing to create directory ${log_path}/shell/socdb/"
		exit 1
	fi
fi
if [ ! -d "${log_path}/vertica/socdb/" ] ; then
	mkdir -p ${log_path}/vertica/socdb/
	if [ $? -ne 0 ]; then
		echo "Failing to create directory ${log_path}/vertica/socdb/"
		exit 1
	fi
fi
## Creation of logfile
if [ -f "$log_file" ]; then
	func_INFO "File ${log_file} exist."
	chmod 644 ${log_file}
fi

func_INFO "###Begining SOCDB_DataLoader version : ${version}."

## Test file to process existing
if [ -z "$(ls ${data_path}/IN)" ]; then
	func_ERREUR "No file to process in ${data_path}/IN"
fi

## Test properties file existing
if [ ! -f "${ctrl_path}/${source}.${base}.${object}.properties" ]; then
	func_ERREUR "File ${ctrl_path}/${source}.${base}.${object}.properties doesn't exist !"
else
	source ${ctrl_path}/${source}.${base}.${object}.properties
	func_INFO "Property file loaded."
	
	# Initialize THRESHOLD
	if [ -n "${THRESHOLD}" ] ; then
		func_INFO "THRESHOLD = ${THRESHOLD}. Process will failed if you have more than ${THRESHOLD} rejected rows."
	else 
		THRESHOLD=1000000
		func_INFO "THRESHOLD = default value (1000000)."
	fi
	# Initialize tablename
	if [ -n "${CUSTOM_TABLENAME}" ] ; then
		tablename=${CUSTOM_TABLENAME^^}
		func_INFO "CUSTOM_TABLENAME parameter ( ${CUSTOM_TABLENAME^^} ) will replace the default tablename value ( ${source^^}_${base^^}_${object^^} )."
	else 
		tablename=${source^^}_${base^^}_${object^^}
	fi
	echo "---tablename---"$tablename
fi

## Creation of ctrl temp directory
if [ ! -d "${ctrl_path}/${property_name}" ] ; then
	func_INFO "${ctrl_path}/${property_name} creation."
	mkdir -p ${ctrl_path}/${property_name}
	if [ $? -ne 0 ]; then
		func_ERREUR "Failing to create directory ${ctrl_path}/${property_name}"
	fi
	chmod -R 750 ${ctrl_path}/${property_name}
fi

## Creation of history directory
if [ ! -d "${data_path}/HISTORY" ] ; then
	func_INFO "${data_path}/HISTORY creation."
	mkdir ${data_path}/HISTORY
	if [ $? -ne 0 ]; then
		func_ERREUR "Failing to create directory ${data_path}/HISTORY."
	fi
	chmod -R 750 ${data_path}/HISTORY
fi

## Creation of ERROR directory
if [ ! -d "${data_path}/ERROR" ] ; then
	func_INFO "${data_path}/ERROR creation."
	mkdir ${data_path}/ERROR
	if [ $? -ne 0 ]; then
		func_ERREUR "Failing to create directory ${data_path}/ERROR."
	fi
	chmod -R 750 ${data_path}/ERROR
fi

## Split preparatory actions
if [[ "${DATABASE}" == "ORACLE" ]]; then
	func_INFO "Target database ORACLE."
	
	### Retrieve Oracle informations (User,Password,ServiceName)
	if [ -n "${DATABASE_SCHEMA}" ]; then
		ORACLE_USER=`grep -w ORACLE_${DATABASE_SCHEMA^^}_User ${CtxCentralizedFilePath} | cut -d= -f2`
		echo "---ORACLE_USER---"$ORACLE_USER
		ORACLE_LISTENER=`grep -w ORACLE_${DATABASE_SCHEMA^^}_ServiceName ${CtxCentralizedFilePath} | cut -d= -f2`
		echo "---ORACLE_LISTENER---"$ORACLE_LISTENER		
		ORACLE_SERVERA=`grep -w ORACLE_${DATABASE_SCHEMA^^}_ServerA ${CtxCentralizedFilePath} | cut -d= -f2`
		echo "---ORACLE_SERVERA---"$ORACLE_SERVERA	
		ORACLE_PORTA=`grep -w ORACLE_${DATABASE_SCHEMA^^}_PortA ${CtxCentralizedFilePath} | cut -d= -f2`
		echo "---ORACLE_PORTA---"$ORACLE_PORTA
		ORACLE_SERVERB=`grep -w ORACLE_${DATABASE_SCHEMA^^}_ServerB ${CtxCentralizedFilePath} | cut -d= -f2`
		echo "---ORACLE_SERVERB---"$ORACLE_SERVERB	
		ORACLE_PORTB=`grep -w ORACLE_${DATABASE_SCHEMA^^}_PortB ${CtxCentralizedFilePath} | cut -d= -f2`
		echo "---ORACLE_PORTB---"$ORACLE_PORTB
		ORACLE_PASSWORD=`grep ${ORACLE_USER} ${PwdCentralisedFilePath}/${ORACLE_USER}.properties | cut -d= -f2`
		#echo "---ORACLE_PASSWORD---"$ORACLE_PASSWORD
		#ORACLE_TOOLS=`grep -w ORACLE_TOOLS ${CtxCentralizedFilePath} | cut -d= -f2`
		#echo "---ORACLE_TOOLS---"$ORACLE_TOOLS		
	else
		func_ERREUR "Variable DATABASE_SCHEMA empty."
	fi
	
	### Test if centralizedContext variables are completed
	if [ -z "${ORACLE_USER}" ] || [ -z "${ORACLE_LISTENER}" ] || [ -z "${ORACLE_SERVERA}" ] || [ -z "${ORACLE_PORTA}" ] || [ -z "${ORACLE_SERVERB}" ] || [ -z "${ORACLE_PORTB}" ] || [ -z "${ORACLE_PASSWORD}" ];  then
		func_ERREUR "Centralized context parameters are not well completed."
	fi
	
	### Generate .ctrl, .sql & tnsnames.ora file
	func_GenerateOracleFiles
	
	#export ORACLE_HOME=$ORACLE_TOOLS
	export ORACLE_HOME
	echo "---ORACLE_HOME---"$ORACLE_HOME
	export TNS_ADMIN=${ctrl_path}/${property_name}
	
	### Test SOCDB_STAT existing
	func_TableExist SOCDB_STAT
	if [ $TE_count -eq 1 ]; then
		func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT exist."
	else
		func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT doesn't exist, we create it."
		
		func_InitializeSOCDB_STAT
		
		func_TableExist SOCDB_STAT
		if [ $TE_count -eq 1 ]; then
			func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT created."
			
			### Grant permission on ${DATABASE_SCHEMA}.SOCDB_STAT
			func_INFO "Grant permission on ${DATABASE_SCHEMA}.SOCDB_STAT to ROLE_${ORACLE_USER}_RO."
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
GRANT SELECT ON ${DATABASE_SCHEMA}.SOCDB_STAT TO ROLE_${ORACLE_USER}_RO;
COMMIT;
EOF
			if [ $? -ne 0 ]; then
				func_WARN "Failing to grant permission on ${DATABASE_SCHEMA}.SOCDB_STAT."
			fi
		else
			func_ERREUR "Table ${DATABASE_SCHEMA}.SOCDB_STAT not created."
		fi
	fi
	
	### Test SOCDB_STAT_DETAIL existing
	func_TableExist SOCDB_STAT_DETAIL
	if [ $TE_count -eq 1 ]; then
		func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL exist."
	else
		func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL doesn't exist, we create it."
		
		func_InitializeSOCDB_STAT_DETAIL
		
		func_TableExist SOCDB_STAT_DETAIL
		if [ $TE_count -eq 1 ]; then
			func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL created."
			
			### Grant permission on ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL
			func_INFO "Grant permission on ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL to ROLE_${ORACLE_USER}_RO."
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
GRANT SELECT ON ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL TO ROLE_${ORACLE_USER}_RO;
COMMIT;
EOF
			if [ $? -ne 0 ]; then
				func_WARN "Failing to grant permission on ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL."
			fi
		else
			func_ERREUR "Table ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL not created."
		fi
	fi
	
	# Add STAT 
	func_InsertSOCDB_STAT Dataloader ${source^^}_${base^^}_${object^^}
	#Get S_ID
	func_GetS_ID Dataloader ${source^^}_${base^^}_${object^^}
	ID=$MAX_ID
	func_INFO "S_ID = $ID"
	
	# Add STAT_DETAIL 
	func_InsertSOCDB_STAT_DETAIL $ID Dataloader Begin
	
	### Test oracle table existing
	func_TableExist ${tablename}
	if [ $TE_count -eq 1 ]; then
		func_INFO "Table ${DATABASE_SCHEMA}.${tablename} exist."
	else
		func_INFO "Table ${DATABASE_SCHEMA}.${tablename} doesn't exist, we create it."
		${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER @${ctrl_path}/${property_name}/${source}.${base}.${object}.sql>>$log_file
		func_TableExist ${tablename}
		if [ $TE_count -eq 1 ]; then
			func_INFO "Table ${DATABASE_SCHEMA}.${tablename} created."
			# Add STAT_DETAIL 
			func_InsertSOCDB_STAT_DETAIL $ID Table_Creation ''
			
			### Grant permission on ${DATABASE_SCHEMA}.${tablename}
			func_INFO "Grant permission on ${DATABASE_SCHEMA}.${tablename} to ROLE_${ORACLE_USER}_RO."
			${ORACLE_HOME}/bin/sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_USER^^}_TNS_DATALOADER <<EOF>>$log_file
whenever sqlerror exit 1
GRANT SELECT ON ${DATABASE_SCHEMA}."${tablename}" TO ROLE_${ORACLE_USER}_RO;
COMMIT;
EOF
			if [ $? -ne 0 ]; then
				func_WARN "Failing to grant permission on ${DATABASE_SCHEMA}.${tablename}."
			fi	
		else
			func_ERREUR "Table ${DATABASE_SCHEMA}.${tablename} not created."
		fi
	fi

	### Creation of DATALOADER directories
	if [ ! -d "${data_path}/DATALOADER" ] ; then
		func_INFO "${data_path}/DATALOADER creation."
		mkdir ${data_path}/DATALOADER
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create directory ${data_path}/DATALOADER"
		fi
		
		func_INFO "${data_path}/DATALOADER/BAD creation."
		mkdir ${data_path}/DATALOADER/BAD
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create directory ${data_path}/DATALOADER/BAD"
		fi
		
		func_INFO "${data_path}/DATALOADER/DISCARD creation."
		mkdir ${data_path}/DATALOADER/DISCARD
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create directory ${data_path}/DATALOADER/DISCARD"
		fi
	fi
	
	chmod -R 750 ${data_path}/DATALOADER
	
	### Purge of DATALOADER directories
	if [ "$(ls -A "${bad_path}" 2> /dev/null)" != "" ]; then
		# The directory is not empty
		func_INFO "Purge old BAD files in ${bad_path}."
		rm ${bad_path}/*_${source}.${base}.${object}.bad
		rm ${bad_path}/*_${source}.${base}.${object}.bad.bad
	fi
	if [ "$(ls -A "${discard_path}" 2> /dev/null)" != "" ]; then
		# The directory is not empty
		func_INFO "Purge old DISCARD files in ${discard_path}."
		rm ${discard_path}/*_${source}.${base}.${object}.dis
	fi
	
elif [[ "${DATABASE}" == "VERTICA" ]]; then
	func_INFO "Target database VERTICA."
	
	### Retrieve Vertica informations (User,Password,vip)
	if [ -n "${DATABASE_SCHEMA}" ]; then
		#provient du fichier de properties
		echo "---VERTICA_USER---"$VERTICA_USER
		VERTICA_DATABASE=`grep -w VERTICA_${DATABASE_SCHEMA}_Database ${CtxCentralizedFilePath} | cut -d= -f2`
		echo "---VERTICA_DATABASE---"$VERTICA_DATABASE
		VERTICA_SERVER=`grep -w VERTICA_${DATABASE_SCHEMA}_Server ${CtxCentralizedFilePath} | cut -d= -f2`
		echo "---VERTICA_SERVER---"$VERTICA_SERVER
		VERTICA_PORT=`grep -w VERTICA_${DATABASE_SCHEMA}_Port ${CtxCentralizedFilePath} | cut -d= -f2`
		echo "---VERTICA_PORT---"$VERTICA_PORT
		VERTICA_PASSWORD=`grep ${VERTICA_USER} ${PwdCentralisedFilePath}/${VERTICA_USER}.properties | cut -d= -f2`
		#echo "---VERTICA_PASSWORD---"$VERTICA_PASSWORD
	else
		func_ERREUR "Variable DATABASE_SCHEMA empty."
	fi
	
	### Test if centralizedContext variables are completed
	if [[ -z ${VERTICA_USER} ]] || [[ -z ${VERTICA_DATABASE} ]] || [[ -z ${VERTICA_SERVER} ]] || [[ -z ${VERTICA_PORT} ]] || [[ -z ${VERTICA_PASSWORD} ]];  then
		func_ERREUR "Centralized context parameters are not well completed."
	fi
	
	### Generate .sql file
	func_GenerateVerticaFiles
	
	### Test SOCDB_STAT_DETAIL existing
	func_TableExist SOCDB_STAT
	if [ $TE_count -eq 1 ]; then
		func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT exist."
	else
		func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT doesn't exist, we create it."
	
		func_InitializeSOCDB_STAT
		
		func_TableExist SOCDB_STAT
		if [ $TE_count -eq 1 ]; then
			func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT created."
		else
			func_ERREUR "Table ${DATABASE_SCHEMA}.SOCDB_STAT not created."
		fi
	fi
	
	### Test SOCDB_STAT_DETAIL existing
	func_TableExist SOCDB_STAT_DETAIL
	if [ $TE_count -eq 1 ]; then
		func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL exist."
	else
		func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL doesn't exist, we create it."
		
		func_InitializeSOCDB_STAT_DETAIL
		
		func_TableExist SOCDB_STAT_DETAIL
		if [ $TE_count -eq 1 ]; then
			func_INFO "Table ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL created."
		else
			func_ERREUR "Table ${DATABASE_SCHEMA}.SOCDB_STAT_DETAIL not created."
		fi
	fi
	
	# Add STAT 
	func_InsertSOCDB_STAT Dataloader ${source^^}_${base^^}_${object^^}
	#Get S_ID
	func_GetS_ID Dataloader ${source^^}_${base^^}_${object^^}
	ID=$MAX_ID
	func_INFO "S_ID = $ID"
	# Add STAT_DETAIL 
	func_InsertSOCDB_STAT_DETAIL $ID Dataloader Begin
	
	### Test vertica table existing
	func_TableExist ${tablename}
	if [ $TE_count -eq 1 ]; then
		func_INFO "Table ${DATABASE_SCHEMA}.${tablename} exist."
	else
		func_INFO "Table ${DATABASE_SCHEMA}.${tablename} doesn't exist, we create it."
		${VERTICA_TOOLS}/vsql -C -U ${VERTICA_USER} -w ${VERTICA_PASSWORD} -h ${VERTICA_SERVER} ${VERTICA_DATABASE} -p ${VERTICA_PORT} -f ${ctrl_path}/${property_name}/${source}.${base}.${object}.sql>> $log_file 2>&1
		func_TableExist ${tablename}
		if [ $TE_count -eq 1 ]; then
			func_INFO "Table ${DATABASE_SCHEMA}.${tablename} created."
			# Add STAT_DETAIL 
			func_InsertSOCDB_STAT_DETAIL $ID Table_Creation ''
		else
			func_ERREUR "Table ${DATABASE_SCHEMA}.${tablename} not created."
		fi
	fi
	
	### Creation of DATALOADER directories
	if [ ! -d "${data_path}/DATALOADER" ] ; then
		func_INFO "${data_path}/DATALOADER creation."
		mkdir ${data_path}/DATALOADER
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create directory ${data_path}/DATALOADER"
		fi
		
		func_INFO "${data_path}/DATALOADER/BAD creation."
		mkdir ${data_path}/DATALOADER/BAD
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to create directory ${data_path}/DATALOADER/BAD"
		fi
	fi
	
	chmod -R 750 ${data_path}/DATALOADER
	
	### Purge of DATALOADER directories
	if [ "$(ls -A "${bad_path}" 2> /dev/null)" != "" ]; then
		# The directory is not empty
		func_INFO "Purge old BAD files in ${bad_path}."
		rm ${bad_path}/*_${source}.${base}.${object}.bad
		
		if [ -e ${bad_path}/*_${source}.${base}.${object}.exc ]; then
			rm ${bad_path}/*_${source}.${base}.${object}.exc
		fi
	fi
	
	
else 
	func_ERREUR "Database ${DATABASE} not available."
fi

# PROCESSING FILES

### Purge of list files	
if [ -f "${ctrl_path}/${property_name}/FileFULL.lst" ]; then
	rm ${ctrl_path}/${property_name}/FileFULL.lst
fi
if [ -f "${ctrl_path}/${property_name}/FileUPDT.lst" ]; then
	rm ${ctrl_path}/${property_name}/FileUPDT.lst
fi
if [ -f "${ctrl_path}/${property_name}/File.lst" ]; then
	rm ${ctrl_path}/${property_name}/File.lst
fi

if ls ${data_path}/IN/* > /dev/null 2>&1; then
	for f in ${data_path}/IN/*
	do
		echo "Filename : $f"
		TABFilenameWithoutPath=`echo $f | awk -F'/' '{ print $15 }'`
		TABLoadMode=`echo $f | awk -F'.' '{ print $7 }'`
		TABFileDate=`echo $f | awk -F'.' '{ print $10 }'`

		if [[ "${TABLoadMode}" == "FULL" ]]; then
			printf "%s\n" "$TABFileDate;$TABFilenameWithoutPath" >> ${ctrl_path}/${property_name}/FileFULL.lst
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to complete file ${ctrl_path}/${property_name}/FileFULL.lst"
			else 
				func_INFO "File ${ctrl_path}/${property_name}/FileFULL.lst completed."
			fi
		fi
		
		if [[ "${TABLoadMode}" == "UPDT" ]]; then
			printf "%s\n" "$TABFileDate;$TABFilenameWithoutPath" >> ${ctrl_path}/${property_name}/FileUPDT.lst	
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to complete file ${ctrl_path}/${property_name}/FileUPDT.lst"
			else 
				func_INFO "File ${ctrl_path}/${property_name}/FileUPDT.lst completed."
			fi
		fi
	done
	if [ -s ${ctrl_path}/${property_name}/FileFULL.lst ]; then
		# The file is not-empty.
		#Process only the most recent FULL file
		printf "%s\n" "`tail -n 1 ${ctrl_path}/${property_name}/FileFULL.lst`" >> ${ctrl_path}/${property_name}/File.lst
		
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to complete file ${ctrl_path}/${property_name}/File.lst"
		else 
			func_INFO "File ${ctrl_path}/${property_name}/File.lst completed."
		fi
		MaxFullDate=`tail -n 1 ${ctrl_path}/${property_name}/FileFULL.lst | awk -F';' '{ print $1 }'`
		echo $MaxFullDate
		if [ -s ${ctrl_path}/${property_name}/FileUPDT.lst ]; then
			# The file is not-empty.
			while IFS= read -r line
			do
				CurrentUpdtDate=`echo $line | awk -F';' '{ print $1 }'`
				#if [ "$CurrentUpdtDate" \> "$MaxFullDate" ] || [ "$CurrentUpdtDate" = "$MaxFullDate" ] ; then
				if [ "$CurrentUpdtDate" \> "$MaxFullDate" ] ; then
					printf "%s\n" "$line" >> ${ctrl_path}/${property_name}/File.lst
					if [ $? -ne 0 ]; then
						func_ERREUR "Failing to complete file ${ctrl_path}/${property_name}/File.lst"
					else 
						func_INFO "File ${ctrl_path}/${property_name}/File.lst completed."
					fi
				fi		
			done < ${ctrl_path}/${property_name}/FileUPDT.lst
		fi
	else
		#only UPDT file(s) to process
		mv ${ctrl_path}/${property_name}/FileUPDT.lst ${ctrl_path}/${property_name}/File.lst
		if [ $? -ne 0 ]; then
			func_ERREUR "Failing to complete file ${ctrl_path}/${property_name}/File.lst"
		else 
			func_INFO "File ${ctrl_path}/${property_name}/File.lst completed."
		fi
	fi
	
	
	## Boucle sur les fichiers à traiter
	while IFS= read -r line
	do
		f=`echo $line | awk -F';' '{ print $2 }'`
		f=${data_path}/IN/$f
		func_INFO "Filename : $f"
		
		# Add STAT_DETAIL 
		func_InsertSOCDB_STAT_DETAIL $ID Input_file `echo $f | awk -F'/' '{ print $15 }'`
		func_InsertSOCDB_STAT_DETAIL $ID NB_Rows_input_file `wc -l $f | cut -d' ' -f-1`
		SaveNb_Line=`wc -l $f | cut -d' ' -f-1`
		
		### ORACLE ###
		if [[ "${DATABASE}" == "ORACLE" ]]; then
			### Replace --FILE_PARAMETER--
			sed -i "s|--FILE_PARAMETER--|$f|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to replace filename in ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl"
			fi
			
			### Replace --LOAD_MODE--
			loadmode=`echo $f | awk -F'.' '{ print $7 }'`
			case "$loadmode" in 
			FULL)
				func_INFO "Loadmode : FULL"
				ORACLE_LOADING_METHODE=TRUNCATE
				;;
			UPDT)
				func_INFO "Loadmode : UPDT"
				ORACLE_LOADING_METHODE=TRUNCATE
				;;
			*) func_ERREUR "Loadmode ($loadmode) not available." ;;
			esac
			
			sed -i "s|--LOAD_MODE--|$ORACLE_LOADING_METHODE|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to replace loadmode in ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl"
			fi
			
			### Replace --FILE_DATE--
			filedate=`echo $f | awk -F'.' '{ print $10 }'`
			##example : `echo '/app/list/data/mapr/cacf-developpement-cluster/AG/DATA/RAW/FILE/TESTDATAFACTORY/TESTDATAFACTORY/7C10R/IN/TESTDATAFACTORY.TESTDATAFACTORY.7C10R.J.00.MFT.FULL.ALL.000.20200310-000000.20200310-160100' | awk -F'.' '{ print $10 }'`
			sed -i "s|--FILE_DATE--|${filedate:0:4}-${filedate:4:2}-${filedate:6:2} ${filedate:9:2}:${filedate:11:2}:${filedate:13:2}|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to replace filedate in ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl"
			fi
			
			### Count rows before
			func_NbRows ${DATABASE_SCHEMA} ${tablename} 1 1
			func_INFO "Row number before loading file `echo $f | awk -F'/' '{ print $15 }'` = $NR_count"
			#Save for later use
			SaveNR_count=$NR_count
			
			# Add STAT_DETAIL 
			func_InsertSOCDB_STAT_DETAIL $ID NBRows_before_loading $NR_count
			func_InsertSOCDB_STAT_DETAIL $ID Loadmode `echo $f | awk -F'.' '{ print $7 }'`

			if [[ "${loadmode}" == "UPDT" ]]; then
				# Check key file
				if [[ -z ${COLUMN_ID} ]]; then
					loadmode=INST
					func_INFO "COLUMN_ID is empty. UPDATE will be done like an INSERT. No duplicates check."
					func_INFO "New Loadmode : INST"
					func_InsertSOCDB_STAT_DETAIL $ID 'New_Loadmode' 'INST'
				fi
			fi
			
			if [[ "${loadmode}" == "UPDT" ]]; then
				# Check key file vs key table
				if [ -n "${COLUMN_ID}" ] && [ -n "${TABLE_KEY}" ] && [ "${COLUMN_ID}" != "${TABLE_KEY}" ]; then
					func_WARN "UPDT mode : File key (COLUMN_ID) and table primary key (TABLE_KEY) are different !"
					# Add STAT_DETAIL
					func_InsertSOCDB_STAT_DETAIL $ID 'WARNING' 'UPDT : COLUMN_ID != TABLE_KEY'
				fi
				
				# Create delete request (composite or single PK)
				FIRST=0
				IFS=','
				read -ra ADDR <<< "${COLUMN_ID}"
				keyLength=${#ADDR[@]}
				if [ ${keyLength} -eq 1 ]; then
					func_INFO "UPDT mode : Single primary key."
					DELETE_REQUEST="DELETE FROM ${DATABASE_SCHEMA}.\"${tablename}\" WHERE ${COLUMN_ID} in (SELECT ${COLUMN_ID} FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\");"
					echo "---DELETE_REQUEST---"$DELETE_REQUEST
				else
					for i in "${ADDR[@]}"; do 
						if [[ $FIRST -eq 0 ]]; then
							FIRST=1
							TO_CONCAT=${i}
						else
							TO_CONCAT="concat("${TO_CONCAT}",$i)"
						fi
					done
					func_INFO "UPDT mode : Composite primary key."
					DELETE_REQUEST="DELETE FROM ${DATABASE_SCHEMA}.\"${tablename}\" WHERE ${TO_CONCAT} IN (SELECT ${TO_CONCAT} FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\");"
					echo "---DELETE_REQUEST---""$DELETE_REQUEST"
				fi
				IFS=' '
			fi
			
			func_LoadOracle
			
			### Count rows after
			func_NbRows ${DATABASE_SCHEMA} ${tablename} 1 1
			func_INFO "Loading file `echo $f | awk -F'/' '{ print $15 }'` : File = $SaveNb_Line / Before = $SaveNR_count / After = $NR_count"
			
			# Add STAT_DETAIL 
			func_InsertSOCDB_STAT_DETAIL $ID NBRows_after_loading $NR_count
			
			# Move file to history
			mv "$f" ${data_path}/HISTORY/
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to move file $f in ${data_path}/HISTORY"
			fi
			
			### Restore --FILE_PARAMETER--
			sed -i "s|$f|--FILE_PARAMETER--|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to restore --FILE_PARAMETER-- in ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl"
			fi
			
			### Restore --LOAD_MODE--
			sed -i "s|$ORACLE_LOADING_METHODE|--LOAD_MODE--|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to restore --LOAD_MODE-- in ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl"
			fi
			### Restore --FILE_DATE--
			sed -i "s|${filedate:0:4}-${filedate:4:2}-${filedate:6:2} ${filedate:9:2}:${filedate:11:2}:${filedate:13:2}|--FILE_DATE--|g" ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to restore --FILE_DATE-- in ${ctrl_path}/${property_name}/${source}.${base}.${object}.ctrl"
			fi
		fi
		
		### VERTICA ###
		if [[ "${DATABASE}" == "VERTICA" ]]; then
			### Initialize loadmode
			loadmode=`echo $f | awk -F'.' '{ print $7 }'`
			case "$loadmode" in 
			FULL)
				func_INFO "Loadmode : FULL"
				;;
			UPDT)
				func_INFO "Loadmode : UPDT"
				;;
			*) func_ERREUR "Loadmode ($loadmode) not available." ;;
			esac
			
			### Initialize filedate
			filedate=`echo $f | awk -F'.' '{ print $10 }'`
			##example : `echo '/app/list/data/mapr/cacf-developpement-cluster/AG/DATA/RAW/FILE/TESTDATAFACTORY/TESTDATAFACTORY/7C10R/IN/TESTDATAFACTORY.TESTDATAFACTORY.7C10R.J.00.MFT.FULL.ALL.000.20200310-000000.20200310-160100' | awk -F'.' '{ print $10 }'`
			
			### Count rows before
			func_NbRows ${DATABASE_SCHEMA} ${tablename} 1 1
			func_INFO "Row number before loading file `echo $f | awk -F'/' '{ print $15 }'` = $NR_count"
			#Save for later use
			SaveNR_count=$NR_count
			
			# Add STAT_DETAIL
			func_InsertSOCDB_STAT_DETAIL $ID NBRows_before_loading $NR_count
			func_InsertSOCDB_STAT_DETAIL $ID Loadmode `echo $f | awk -F'.' '{ print $7 }'`

			if [[ "${loadmode}" == "UPDT" ]]; then
				# Check key file
				if [[ -z ${COLUMN_ID} ]]; then
					loadmode=INST
					func_INFO "COLUMN_ID is empty. UPDATE will be done like an INSERT. No duplicates check."
					func_INFO "New Loadmode : INST"
					func_InsertSOCDB_STAT_DETAIL $ID 'New_Loadmode' 'INST'
				fi
			fi
			
			if [[ "${loadmode}" == "UPDT" ]]; then
				# Check key file vs key table
				if [ -n "${COLUMN_ID}" ] && [ -n "${TABLE_KEY}" ] && [ "${COLUMN_ID}" != "${TABLE_KEY}" ]; then
					func_WARN "UPDT mode : File key (COLUMN_ID) and table primary key (TABLE_KEY) are different !"
					# Add STAT_DETAIL
					func_InsertSOCDB_STAT_DETAIL $ID 'WARNING' 'UPDT : COLUMN_ID != TABLE_KEY'
				fi
					
				# Create delete request (composite or single PK)
				FIRST=0
				IFS=','
				read -ra ADDR <<< "${COLUMN_ID}"
				keyLength=${#ADDR[@]}
				if [ ${keyLength} -eq 1 ]; then
					func_INFO "UPDT mode : Single primary key."
					DELETE_REQUEST="DELETE FROM ${DATABASE_SCHEMA}.\"${tablename}\" WHERE ${COLUMN_ID} in (SELECT ${COLUMN_ID} FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\");"
					echo "---DELETE_REQUEST---"$DELETE_REQUEST
				else
					for i in "${ADDR[@]}"; do 
						if [[ $FIRST -eq 0 ]]; then
							FIRST=1
							TO_CONCAT=${i}
						else
							TO_CONCAT="concat("${TO_CONCAT}",$i)"
						fi
					done
					func_INFO "UPDT mode : Composite primary key."
					DELETE_REQUEST="DELETE FROM ${DATABASE_SCHEMA}.\"${tablename}\" WHERE ${TO_CONCAT} IN (SELECT ${TO_CONCAT} FROM ${DATABASE_SCHEMA}.\"${tablename}_DATALOADER\");"
					echo "---DELETE_REQUEST---""$DELETE_REQUEST"
				fi
				IFS=' '
			fi
			
			func_LoadVertica
			
			### Count rows after
			func_NbRows ${DATABASE_SCHEMA} ${tablename} 1 1
			func_INFO "Loading file `echo $f | awk -F'/' '{ print $15 }'` : File = $SaveNb_Line / Before = $SaveNR_count / After = $NR_count"
			# Add STAT_DETAIL 
			func_InsertSOCDB_STAT_DETAIL $ID NBRows_after_loading $NR_count
			
			# Move file to history
			mv "$f" ${data_path}/HISTORY/
			if [ $? -ne 0 ]; then
				func_ERREUR "Failing to move file $f in ${data_path}/HISTORY"
			fi
		fi
	done < ${ctrl_path}/${property_name}/File.lst

	# Move file not processed to history	
	if ls ${data_path}/IN/* > /dev/null 2>&1; then	
		for f in ${data_path}/IN/*
		do
			func_INFO "File `echo $f | awk -F'/' '{ print $15 }'` not processed, move to HISTORY"
			mv "$f" ${data_path}/HISTORY/
				if [ $? -ne 0 ]; then
					func_ERREUR "Failing to move file $f in ${data_path}/HISTORY"
				fi
			func_InsertSOCDB_STAT_DETAIL $ID File_Not_Processed `echo $f | awk -F'/' '{ print $15 }'`
		done
	fi	
	
	# Add STAT_DETAIL 
	func_InsertSOCDB_STAT_DETAIL $ID Dataloader End
	
	# Update STAT 
	func_UpdateSOCDB_STAT $ID 1
	
else
	echo "No file to process in ${data_path}/IN/"
fi

# END PROGRAMM
func_INFO "###Ending SOCDB_DataLoader version : ${version}."
