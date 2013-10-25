. ./init.sh

mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"

sleep 1
./stop_s1.sh
./start_s1.sh --log-slave-updates
mysql $S1 -e "stop slave io_thread;start slave io_thread"

mysql $S2 -e "stop slave"
is_gtid_supported
if test $? = 1
then
mysql $S2 -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox'"
else
mysql $S2 -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_log_file='mysql-bin.000001', master_log_pos=4"
fi

mysql $S2 -e "start slave"

./stop_s2.sh
./start_s2.sh --log-slave-updates
mysql $S2 -e "stop slave io_thread;start slave io_thread"

mysql $S3 -e "stop slave"
is_gtid_supported
if test $? = 1
then
mysql $S3 -e "change master to master_host='127.0.0.1', master_port=$S2P, master_user='rsandbox', master_password='rsandbox'"
else
mysql $S3 -e "change master to master_host='127.0.0.1', master_port=$S2P, master_user='rsandbox', master_password='rsandbox', master_log_file='mysql-bin.000001', master_log_pos=4"
fi
mysql $S3 -e "start slave"

sleep 5

check_master $0 $S1P $MP
check_master $0 $S2P $S1P
check_master $0 $S3P $S2P
check_master $0 $S4P $MP

mysql $M test -e "insert into t1 values(3, 300, 'aaaaaa')"

masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?
masterha_check_repl --conf=mha_test_multi.cnf > /dev/null 2>&1
fail_if_zero $0 $?

mysql $S1 -e "set global read_only=1"

masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh
fail_if_zero $0 $?
masterha_check_repl --conf=mha_test_multi.cnf > /dev/null 2>&1
fail_if_zero $0 $?

./stop_s2.sh

masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh
fail_if_zero $0 $?
masterha_check_repl --conf=mha_test_multi.cnf > /dev/null 2>&1
fail_if_zero $0 $?

./kill_m.sh
./run.sh --conf=mha_test_multi.cnf
fail_if_zero $0 $?

echo "$0 [Pass]"
