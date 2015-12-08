#/bin/sh

. ./env.sh

SANDBOX_HOME_ESC=`echo $SANDBOX_HOME | sed -e 's/\//\\\\\\//g'`

for tmpl in `ls mha_test*.cnf.tmpl`
do
cnf=${tmpl%.tmpl}
sed -e "s/##VERSION_DIR##/$VERSION_DIR/g" -e "s/##SANDBOX_HOME##/$SANDBOX_HOME_ESC/g" \
  -e "s:##CLIENT_BINDIR##:$CLIENT_BINDIR:" -e "s:##CLIENT_LIBDIR##:$CLIENT_LIBDIR:" \
  $tmpl > $cnf
done

$SANDBOX_HOME/send_kill_all  > bootstrap.log 2>&1
$SANDBOX_HOME/clear_all  > bootstrap.log 2>&1
export MYSQL_USER=""
export MYSQL_PWD=""
make_replication_sandbox --how_many_slaves=4 --upper_directory=$SANDBOX_HOME --sandbox_base_port=$MP  $VERSION >> bootstrap.log 2>&1
export MYSQL_USER=root
export MYSQL_PWD=msandbox
if [ "A$INIT_SCRIPT" != "A" ]; then
  eval $INIT_SCRIPT
fi

rm -f /var/tmp/*127.0.0.1_*.binlog
rm -f /var/tmp/*127.0.0.1_*.log
rm -f /var/tmp/mha_test*
sleep 1
mysql $M -e "source grant.sql"
mysql $M -e "source grant_nopass.sql"
mysql $S1 -e "source grant_nopass.sql"
mysql $S2 -e "source grant_nopass.sql"
mysql $S3 -e "source grant_nopass.sql"
mysql $S4 -e "source grant_nopass.sql"
mysql $M test -e "create table t1 (id int primary key, value int, value2 text); insert into t1 values(1, 100, 'abc');"

wait_until_count $0 $S1P 1
wait_until_count $0 $S2P 1
wait_until_count $0 $S3P 1
wait_until_count $0 $S4P 1

if [ "A$USE_GTID_AUTO_POS" != "A" ]; then
  mysql $S1 -e "stop slave; change master to master_auto_position=1; start slave" > /dev/null 2>&1
  mysql $S2 -e "stop slave; change master to master_auto_position=1; start slave" > /dev/null 2>&1
  mysql $S3 -e "stop slave; change master to master_auto_position=1; start slave" > /dev/null 2>&1
  mysql $S4 -e "stop slave; change master to master_auto_position=1; start slave" > /dev/null 2>&1
fi

