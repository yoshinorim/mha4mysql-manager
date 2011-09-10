. ./init.sh

mysql $S2 -e "stop slave sql_thread"
mysql $M test -e "insert into t1 values (100, 100, 100)"
check_sql_stop $0 $S2P

./run_bg.sh &
wait_until_manager_start $0
check_sql_stop $0 $S2P

masterha_check_status --conf=$CONF > /dev/null
fail_if_nonzero $0 $?

masterha_stop --conf=$CONF > /dev/null

check_sql_stop $0 $S2P

./kill_m.sh
./run.sh

check_sql_yes $0 $S2P

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 3

