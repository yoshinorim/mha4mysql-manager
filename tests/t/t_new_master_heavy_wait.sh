. ./init.sh

masterha_manager --conf=$CONF --log_output=manager.log --skip_ssh_check >> manager.log 2>&1 &
manager1_pid=$!
wait_until_manager_start $0 

mysql $S1 test -e "set global read_only=1"
mysql $S1 test -e "SELECT *, sleep(200) from t1" > /dev/null 2>&1 &
mysql_pid=$!
./stop_m.sh
./waitpid $manager1_pid 50
rc1=$?

kill $mysql_pid
wait $mysql_pid 2> /dev/null

if [ $rc1 -eq 10 ]; then
  masterha_stop --conf=$CONF >> manager.log 2>&1
  echo "$0 [Pass(wait)]"
  exit 1
fi
if [ $rc1 -ne 0 ]; then
  masterha_stop --conf=$CONF >> manager.log 2>&1
  echo "$0 [Fail (got error $rc)]"
  exit 1
fi

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 2
