. ./init.sh

POS1=`get_binlog_position $S1P`
is_gtid_supported
if test $? = 1
then
mysql $M -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_auto_position=1; start slave"
else
mysql $M -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_log_file='mysql-bin.000001', master_log_pos=$POS1; start slave"
fi

mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"
sleep 1
POS2=`get_binlog_position $S1P`
mysql $S1 -e "set global read_only=1"

mysql $S2 -e "stop slave"
is_gtid_supported
if test $? = 1
then
mysql $S2 -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox'"
else
mysql $S2 -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_log_file='mysql-bin.000001', master_log_pos=$POS2"
fi
mysql $S2 -e "start slave"


check_master $0 $S2P $S1P

masterha_master_switch --master_state=alive --interactive=0 --conf=$CONF --new_master_host=127.0.0.1 --new_master_port=$S1P --orig_master_is_new_slave > switch.log 2>&1
fail_if_zero $0 $?

masterha_master_switch --master_state=alive --interactive=0 --conf=mha_test_multi.cnf --new_master_host=127.0.0.1 --new_master_port=$S1P --orig_master_is_new_slave > switch.log 2>&1
fail_if_nonzero $0 $?

check_master $0 $S2P $S1P
check_master $0 $MP $S1P
mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
check_count $0 $MP 3
./check $0 3
