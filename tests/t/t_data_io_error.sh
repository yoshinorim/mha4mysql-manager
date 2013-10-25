. ./init.sh

mysql $M test -e "insert into t1 values (100, 100, 100)"
mysql $S1 test -e "stop slave io_thread"
mysql $S2 test -e "stop slave io_thread"
mysql $S3 test -e "stop slave io_thread"
mysql $S4 test -e "stop slave io_thread"
mysql $M test -e "insert into t1 values (101, 100, 100)"

masterha_manager --conf=$CONF --log_output=manager.log --skip_ssh_check --ignore_binlog_server_error > manager.log 2>&1 &
manager_pid=$!
wait_until_manager_start $0

masterha_check_status --conf=$CONF > /dev/null
fail_if_nonzero $0 $?
mkdir $SANDBOX_HOME/rsandbox_$VERSION_DIR/master/data_tmp
mv $SANDBOX_HOME/rsandbox_$VERSION_DIR/master/data/mysql-bin* $SANDBOX_HOME/rsandbox_$VERSION_DIR/master/data_tmp/

./kill_m.sh
./waitpid $manager_pid 50
rc1=$?
rm -rf $SANDBOX_HOME/rsandbox_$VERSION_DIR/master/data_tmp

if [ $rc1 -ne 0 ]; then
  masterha_stop --conf=$CONF >> manager.log 2>&1
  echo "$0 [Fail (got error $rc)]"
  exit 1
fi

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 3

