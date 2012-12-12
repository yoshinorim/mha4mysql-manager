#/bin/sh

. ./env.sh

SANDBOX_HOME_ESC=`echo $SANDBOX_HOME | sed -e 's/\//\\\\\\//g'`

for tmpl in `ls mha_test*.cnf.tmpl`
do
cnf=${tmpl%.tmpl}
sed -e "s/##VERSION_DIR##/$VERSION_DIR/g" -e "s/##SANDBOX_HOME##/$SANDBOX_HOME_ESC/g" $tmpl > $cnf
done

$SANDBOX_HOME/send_kill_all  > bootstrap.log 2>&1
$SANDBOX_HOME/clear_all  > bootstrap.log 2>&1
make_replication_sandbox --how_many_slaves=4 --upper_directory=$SANDBOX_HOME --sandbox_base_port=$MP  $VERSION >> bootstrap.log 2>&1

if [ "A$INIT_SCRIPT" != "A" ]; then
  eval $INIT_SCRIPT
fi

rm -f /var/tmp/*127.0.0.1_*.binlog
rm -f /var/tmp/*127.0.0.1_*.log
rm -f /var/tmp/mha_test*

mysql $M -e "source grant.sql"
mysql $M -e "source grant_nopass.sql"
mysql $M test -e "create table t1 (id int primary key, value int, value2 text) engine=innodb; insert into t1 values(1, 100, 'abc');"

wait_until_count $0 $S1P 1
wait_until_count $0 $S2P 1
wait_until_count $0 $S3P 1
wait_until_count $0 $S4P 1

