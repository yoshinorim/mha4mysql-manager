. ./init.sh

mysql $M test -e "insert into t1 values (100, 100, 100)"

./run_bg.sh --conf=$CONF_IGNORE
wait_until_manager_start $0 --conf=$CONF_IGNORE
./stop_s4.sh
./kill_m.sh
sleep 50

masterha_stop --conf=$CONF_IGNORE > /dev/null
./start_s4.sh

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
check_master $0 $S4P $MP
check_count $0 $S4P 2
./check $0 3 "h=127.0.0.1,P=$S1P  h=127.0.0.1,P=$S2P h=127.0.0.1,P=$S3P"

