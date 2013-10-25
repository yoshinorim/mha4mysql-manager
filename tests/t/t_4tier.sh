. ./init.sh

mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"

sleep 1
./stop_s1.sh
./start_s1.sh --log-slave-updates
mysql $S1 -e "stop slave io_thread;start slave io_thread"
FILE1=`get_binlog_file $S1P`
POS1=`get_binlog_position $S1P`
mysql $S2 -e "stop slave"
is_gtid_supported
if test $? = 1
then
mysql $S2 -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox'"
else
mysql $S2 -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_log_file=\"$FILE1\", master_log_pos=$POS1"
fi

mysql $S2 -e "start slave"

./stop_s2.sh
./start_s2.sh --log-slave-updates
mysql $S2 -e "stop slave io_thread;start slave io_thread"
FILE2=`get_binlog_file $S2P`
POS2=`get_binlog_position $S2P`

mysql $S3 -e "stop slave"
is_gtid_supported
if test $? = 1
then
mysql $S3 -e "change master to master_host='127.0.0.1', master_port=$S2P, master_user='rsandbox', master_password='rsandbox'"
else
mysql $S3 -e "change master to master_host='127.0.0.1', master_port=$S2P, master_user='rsandbox', master_password='rsandbox', master_log_file=\"$FILE2\", master_log_pos=$POS2"
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

mysql $S2 -e "set global read_only=1"

masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh
fail_if_zero $0 $?
masterha_check_repl --conf=mha_test_multi.cnf > /dev/null 2>&1
fail_if_nonzero $0 $?

./kill_m.sh
./run.sh --conf=mha_test_multi.cnf
fail_if_nonzero $0 $?

check_master $0 $S2P $S1P
check_master $0 $S3P $S2P
check_master $0 $S4P $S1P

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 4 
