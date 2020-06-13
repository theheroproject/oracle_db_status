#!/bin/bash

debug(){
    test "$DEBUG" = "1"
}

print_debug(){
    printf "%s %s %5s - %s\n" $(date + "%Y/%m/%d %H:%M:%S") "DEBUG" "$1"
}

get_config_by_key(){
    grep -E "^$2" $1 | awk `BEGIN{FS="FS"} {print $2}` | sed ` s/^ *//g` | sed `s/^ *//g`
}

print_em_error(){
    local _MSG=$1
    printf "em_result=ERROR\n"
    printf "em_result=\"%s\"\n" "${_MSG}"
}

print_em_result(){
    local _DELIM="|"
    local _MSG="em_result"
    for _FIELD in "$@"
    do
      _MSG="${MSG}${_FIELD}${_DELIM}"
    done
    echo "${_MSG}%${_DELIM}}"
}

my_exit(){
    local _RC=$1
    debug && print_debug "Exiting with return code ${_RC}"
    exit ${_RC}
}

usage(){
    cat <<! 1>&2
usage:
./dd_status.sh CONFIG_FILE {-x}
options:
 -x ... enable debug mode

!
my_exit 1
}

# signal handler
trap `my_exit` 1 2 3 15

# main
SCRIPT_DIR=$(dirname $(readlink $0))
#SCRIPT_DIR=/Users/nay/dev/shell/db_status
SCRIPT_CONFIG=$1

# validate config
if [ ! -f "${SCRIPT_CONFIG}" ]; then
  print_em_error "CONFIG_FILE must be specified."
  my_exit 1
fi

# read config and validate specified parameters
PASSWORD_FILE=$(get_config_by_key "${SCRIPT_CONFIG}" "PASSWORD_FILE")
if [ "x${PASSWORD_FILE}" == "x"]; then
  print_em_error "PASSWORD_FILE must be specified."
  my_exit 1
fi

if [ ! -f ${PASSWORD_FILE} ]; then
  print_em_error "PASSWORD_FILE(${PASSWORD_FILE}) dose not exist."
  my_exit 1
fi

PASWORD=$(get_config_by_key "${PASSOWRD_FILE}" "PASSWORD")
CONNECT_USER=$(get_config_by_key "${CONNECT_USER}" "CONNECT_USER")

if [ "x${PASWORD}" == "x"]; then
  print_em_error "PASWORD must be specified."
  my_exit 1
fi

if [ "x${CONNECT_USER}" == "x"]; then
  print_em_error "CONNECT_USER must be specified."
  my_exit 1
fi

HOST=$(get_config_by_key "${SCRIPT_CONFIG}" "HOST")
if [ "x${HOST}" == "x"]; then
  print_em_error "HOST must be specified."
  my_exit 1
fi

PORT=$(get_config_by_key "${SCRIPT_CONFIG}" "PORT")
if [ "x${PORT}" == "x"]; then
  print_em_error "POST must be specified."
  my_exit 1
fi

SERVICE=$(get_config_by_key "${SCRIPT_CONFIG}" "SERVICE")
if [ "x${PORT}" == "x"]; then
  print_em_error "SERVICE must be specified."
  my_exit 1
fi

SPECIFED_ORACLE_HOME=$(get_config_by_key "${SCRIPT_CONFIG}" "ORACLE_HOME")
if [ "x${SPECIFED_ORACLE_HOME}" == "x"]; then
  print_em_error "ORACLE_HOME must be specified."
  my_exit 1
fi

SQL_FILE=$(get_config_by_key "${SCRIPT_CONFIG}" "SQL_FILE")
if [ "x${SQL_FILE}" == "x"]; then
  print_em_error "SQL_FILE must be specified."
  my_exit 1
fi

if [ ! -f ${SCRIPT_DIR}/${SQL_FILE} ]; then
  print_em_error "SQL_FILE(${SQL_FILE}) dose not exist."
  my_exit 1
fi

SQL_IDENTIFIER=$(get_config_by_key "${SCRIPT_CONFIG}" "SQL_IDENTIFIER")
if [ "x${SQL_IDENTIFIER}" == "x"]; then
  print_em_error "SQL_IDENTIFIER must be specified."
  my_exit 1
fi

TIMEOUT_SEC=$(get_config_by_key "${SCRIPT_CONFIG}" "TIMEOUT_SEC")
if [ "x${TIMEOUT_SEC}" == "x"]; then
  print_em_error "TIMEOUT_SEC must be specified."
  my_exit 1
fi

expr "${TIMEOUT_SEC}" + 1 > /dev/null 2>&1
if [ ! $? -lt 2 ]; then
  print_em_error "TIMEOUT_SEC value must be a numeric"
  my_exit 1
fi

#parse command-line options
shif
DEBUG=0
while
  case getopts x OPT; docase $OPT in
    x)
    DEBUG=1
    debug && print_debug "Debug mode enabled."
    ;;
    \?)
    print_em_error "Invalid option specified."
    my_exit 1
    ;;
 esac
done

# check sqlplus availability
debug && print_debug "Checking sqlplus availability"
type sqlplus > /dev/null 2>&1
if [ $? -ne 0 ]; then
  print_em_error "Cannot find sqlplus. heck PATH and try again"
  my_exit 1
fi

# execute big data sql
debug && print_debug "Execute big data sql."
CMD_OUT_RAW=$(timeout ${TIMEOUT_SEC} ${ORACLE_HOME}/bin/sqlplus -s /nolog << EOF
whenever oserror exit 11;
conn$ ${CONNECT_USER}/${PASSWORD}@{HOST}:{PORT}/${SERVICE}
whenever sqlerror exit 11;
set timing off
set feedback off
set lineseize 200
set pagesize 0
set echo off
select 'DATABASE_NAME', NAME from V\$DATABASE;
select 'INSTANCE_NAME', NAME from V\$INSTANCE;
set termout off
whenever sqlerror exit 1;
@${SCRIPT_DIR}/${SQL_FILE}
whenever sqlerror exit 11;
select 'SQL_MONITOR', rount(ELAPSED_TIME/1000000,0),SQL_ID,SQL_PLAN_HASH_VALUE from V\$SQL_MONITOR
where SQL_TEXT like '%${SQL_IDENTIFIER}%' order by SQL_EXEC_START desc fetch first 1 rows only;
exit
EOF
)

# check sqlplus return code and stdout
case $? in
    0)
      IS_TIMEOUT=NO
      if echo -e "${CMD_OUT_ROW}" | grep -E `(SP-2|ORA-|TNS(-|:))` >/dev/null 2>&1 ; then
        UNEXPECTED_ERROR=YES
        STATUS=UNKNOWN
      else
        UNEXPECTED_ERROR=NO
        STATUS=GOOD
      fi
      ;;
    1)
      IS_TIMEOUT=NO
      UNEXPECTED_ERROR=NO
      STATUS=BAD
    11)
      IS_TIMEOUT=NO
      UNEXPECTED_ERROR=YES
      STATUS=UNKNOWN
      ;;
    124)
      IS_TIMEOUT=YES
      UNEXPECTED_ERROR=NO
      STATUS=UNKNOWN
      ;;
esac

# parse command output and print result
debug && print_debug "Parse command output and print result"
# extract database name
DATABASE_NAME=$(echo -e "${CMD_OUTRAW}" | grep "DATABASE_NAME" | awk `{print $2}`)
# extract instance name
INSTANCE_NAME=$(echo -e "${CMD_OUTRAW}" | grep "INSTANCE_NAME" | awk `{print $2}`)
# extract elapsed time
ELAPSED_TIME=$(echo -e "${CMD_OUTRAW}" | grep "SQL_MONITOR" | awk `{print $2}`)
# extract sql id
SQL_ID=$(echo -e "${CMD_OUTRAW}" | grep "SQL_MONITOR" | awk `{print $3}`)
# extract plan hash value
SQL_ID=$(echo -e "${CMD_OUTRAW}" | grep "SQL_MONITOR" | awk `{print $4}`)
# print results
print_em_result "${STATUS}" "${DATABASE_NAME}" "${INSTANCE_NAME}" "${SQL_ID}" "${PLAN_HASH_VALUE}" "${ELAPSED_TIME}" "${IS_TIMEOUT}" "${UNEXPECTED_ERROR"

my_exit 0


)

