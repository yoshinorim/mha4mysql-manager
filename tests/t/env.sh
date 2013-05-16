#!/bin/sh

if [ "A$VERSION" = "A" ]; then
  export VERSION=5.5.16
fi

if [ "A$VERSION_DIR" = "A" ]; then
  export VERSION_DIR=5_5_16
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
export CLIENT_BINDIR=""
export CLIENT_LIBDIR=""
if [ "A$CUSTOM_CLIENTS" = "Ayes" ]; then
  export CLIENT_BINDIR="client_bindir=/opt/mysql/$VERSION/bin"
#  export CLIENT_LIBDIR="client_libdir=/opt/mysql/$VERSION/lib/mysql"
elif [ "A$CUSTOM_CLIENTS" = "Abad" ]; then
  export CLIENT_BINDIR="client_bindir=/opt/mysql/$VERSION"
  export CLIENT_LIBDIR="client_libdir=/opt/mysql/$VERSION"
else
  export CLIENT_BINDIR=""
  export CLIENT_LIBDIR=""
fi

fail_if_zero() {
  if test $2 -eq 0 ; then
    echo "$1 [Fail] (expected non-zero exit code, but $2 returned)"
    exit 1
  fi
}

fail_if_nonzero() {
  if test $2 -ne 0 ; then
    echo "$1 [Fail] (expected zero exit code, but $2 returned)"
    exit 1
  fi
}

fail_if_nonN() {
  if test $2 -ne $3 ; then
    echo "$1 [Fail] (expected $3 exit code, but $2 returned)"
    exit 1
  fi
}

fail_if_empty() {
  if test ! -s $2 ; then
    echo "$1 [Fail] ($2 is empty)"
    exit 1
  fi
}

fail_if_nonempty() {
  if test -s $2 ; then
    echo "$1 [Fail] ($2 is not empty)"
    exit 1
  fi
}

is_read_only() {
READ_ONLY=`mysql -h127.0.0.1 --port=$2 -e "select @@global.read_only\G" | grep read_only | awk '{print $2}'`
  if [ "$READ_ONLY" = "1" ]; then
    return
  else
    echo "$1 [Fail (read_only is 0)]"
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

wait_until_manager_start() {
  i=1
  while :
  do
    masterha_check_status --conf=$CONF $2 > /dev/null 2>&1
    RC=$?
    if [ "$RC" = "0" ]; then
      break
    fi
    i=`expr $i + 1`
    if [ $i -gt 120 ]; then
      echo "$1 [Fail (master_check_status does not become running within 120 seconds)]"
      exit 1
    fi
    sleep 1
  done
}

wait_until_count() {
  i=1
  while :
  do
ROW_COUNT=`mysql -h127.0.0.1 --port=$2 test -e "select count(*) from t1\G" 2> /dev/null | grep count | awk {'print $2'}`
    if [ "$ROW_COUNT" = $3 ]; then
      break
    fi
    i=`expr $i + 1`
    if [ $i -gt 120 ]; then
      echo "$1 [Fail (wait timeout)]"
      exit 1
    fi
    sleep 1
  done
}
