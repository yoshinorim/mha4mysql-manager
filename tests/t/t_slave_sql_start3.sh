. ./init.sh

mysql $S2 test -e "stop slave sql_thread"
mysql $S2 test -e "insert into t1 values (100, 100, 100)"
mysql $M test -e "insert into t1 values (100, 100, 100)"
check_sql_stop $0 $S2P

./run_bg.sh &
wait_until_manager_start $0
check_sql_stop $0 $S2P

masterha_check_status --conf=$CONF  > /dev/null
masterha_stop --conf=$CONF  > /dev/null

check_sql_stop $0 $S2P

./kill_m.sh
./run.sh

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 3 "h=127.0.0.1,P=$S1P  h=127.0.0.1,P=$S3P h=127.0.0.1,P=$S4P"


