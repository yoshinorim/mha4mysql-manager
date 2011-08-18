#/bin/sh

. ./env.sh

SANDBOX_HOME_ESC=`echo $SANDBOX_HOME | sed -e 's/\//\\\\\\//g'`
sed -e "s/##VERSION_DIR##/$VERSION_DIR/g" -e "s/##SANDBOX_HOME##/$SANDBOX_HOME_ESC/g" mha_test.cnf.tmpl > $CONF
sed -e "s/##VERSION_DIR##/$VERSION_DIR/g" -e "s/##SANDBOX_HOME##/$SANDBOX_HOME_ESC/g" mha_test_latest.cnf.tmpl > $CONF_LATEST
sed -e "s/##VERSION_DIR##/$VERSION_DIR/g" -e "s/##SANDBOX_HOME##/$SANDBOX_HOME_ESC/g" mha_test_ignore.cnf.tmpl > $CONF_IGNORE

$SANDBOX_HOME/send_kill_all  > bootstrap.log 2>&1
$SANDBOX_HOME/clear_all  > bootstrap.log 2>&1
make_replication_sandbox --how_many_slaves=4 --upper_directory=$SANDBOX_HOME --sandbox_base_port=$MP  $VERSION >> bootstrap.log 2>&1

mysql $M test -e "create table t1 (id int primary key, value int, value2 text) engine=innodb; insert into t1 values(1, 100, 'abc');"

