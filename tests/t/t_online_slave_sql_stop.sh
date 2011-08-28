. ./init.sh

mysql $S2 test -e "stop slave sql_thread"
check_sql_stop $0 $S2P

masterha_master_switch --master_state=alive --interactive=0 --conf=$CONF --new_master_host=127.0.0.1 --new_master_port=$S1P > switch.log 2>&1
fail_if_zero $0 $?

mysql $S2 test -e "start slave sql_thread"
check_sql_yes $0 $S2P

masterha_master_switch --master_state=alive --interactive=0 --conf=$CONF --new_master_host=127.0.0.1 --new_master_port=$S1P > switch.log 2>&1
fail_if_nonzero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 2
