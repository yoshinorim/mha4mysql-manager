. ./init.sh
skip_if_gtid $0

mysql $M test -e "insert into t1 values (10, 100, 100)"
mysql $M test -e "insert into t1 values (11, 100, 100)"
# S1 SQL thread stops with error
mysql $S1 test -e "insert into t1 values (100, 100, 100)"
mysql $M test -e "insert into t1 values (100, 100, 100)"
sleep 1
check_sql_stop $0 $S1P

masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?

./run.sh
fail_if_zero $0 $?

mysql $S1 test -e "delete from t1 where id=100"

masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?

mysql $S1 test -e "start slave sql_thread"

masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_nonzero $0 $?

./kill_m.sh
./run.sh

check_sql_yes $0 $S2P

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 5


