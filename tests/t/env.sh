#!/bin/sh

if [ "A$VERSION" = "A" ]; then
  export VERSION=5.5.15
fi

if [ "A$VERSION_DIR" = "A" ]; then
  export VERSION_DIR=5_5_15
fi

if [ "A$USE_ROW_FORMAT" = "A" ]; then
  export NODE_OPTIONS="--my_file=my.cnf"
else
  export NODE_OPTIONS="--my_file=my-row.cnf"
fi

export SANDBOX_HOME=/opt/mysql/sandbox_data
export SANDBOX_AS_ROOT=1
export MYSQL_USER=root
export MYSQL_PWD=msandbox
export M="--host=127.0.0.1 --port=10000"
export S1="--host=127.0.0.1 --port=10001"
export S2="--host=127.0.0.1 --port=10002"
export S3="--host=127.0.0.1 --port=10003"
export S4="--host=127.0.0.1 --port=10004"
export MP=10000
export S1P=10001
export S2P=10002
export S3P=10003
export S4P=10004
export CONF=mha_test.cnf
export CONF_LATEST=mha_test_latest.cnf
export CONF_IGNORE=mha_test_ignore.cnf

fail_if_zero() {
  if test $2 -eq 0 ; then
    echo "$1 [Fail]"
    exit 1
  fi
}

fail_if_nonzero() {
  if test $2 -ne 0 ; then
    echo "$1 [Fail]"
    exit 1
  fi
}

check_sql_yes() {
SQL_STATUS=`mysql -h127.0.0.1 --port=$2 -e "show slave status\G" | grep Slave_SQL_Running: | awk '{print $2}'`
  if [ "$SQL_STATUS" = "Yes" ]; then
    return
  else
    echo "$1 [Fail (slave not running)]"
    exit 1
  fi
}

check_sql_stop() {
SQL_STATUS=`mysql -h127.0.0.1 --port=$2 -e "show slave status\G" | grep Slave_SQL_Running: | awk '{print $2}'`
  if [ "$SQL_STATUS" = "No" ]; then
    return
  else
    echo "$1 [Fail (slave not stop)]"
    exit 1
  fi
}

check_master() {
MASTER_PORT=`mysql -h127.0.0.1 --port=$2 -e "show slave status\G" | grep Master_Port: | awk '{print $2}'`
  if [ "$MASTER_PORT" = "$3" ]; then
    return
  else
    echo "$1 [Fail (Master Port $2 is not equal to $3)]"
    exit 1
  fi
}

check_count() {
COUNT=`mysql -h127.0.0.1 --port=$2 test -e "select count(*) as value from t1\G" | grep value | awk '{print $2}'`
  if [ "$COUNT" = "$3" ]; then
    return
  else
    echo "$1 [Fail (COUNT $COUNT is not equal to expected count $3)]"
    exit 1
  fi
}

check_relay_purge() {
PURGE=`mysql -h127.0.0.1 --port=$2 test -e "select @@global.relay_log_purge\G" | grep global | awk '{print $2}'`
  if [ "$PURGE" = "$3" ]; then
    return
  else
    echo "$1 [Fail (relay_log_purge $PURGE is not equal to expected value $3)]"
    exit 1
  fi
}

