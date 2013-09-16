. ./init.sh

skip_if_gtid $0
# commits every 1000 rows
# sql thread stops normally (simulating offline backup job)

mysql $S1 test -e "set global relay_log_purge=0"
mysql $S2 test -e "set global relay_log_purge=0"
mysql $S3 test -e "set global relay_log_purge=0"
mysql $S4 test -e "set global relay_log_purge=0"

mysql $S3 test -e "flush logs"
mysql $S3 test -e "flush logs"


perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 2 1000 0

mysql $S2 test -e "stop slave sql_thread"
mysql $S2 test -e "insert into t1 values (99950, 100, 100)"
check_sql_stop $0 $S2P

perl tran_insert.pl $MP $MYSQL_USER $MYSQL_PWD 1001 100000 1000

./run_bg.sh &
wait_until_manager_start $0
check_sql_stop $0 $S2P
masterha_check_status --conf=$CONF > /dev/null
fail_if_nonzero $0 $?

masterha_stop --conf=$CONF > /dev/null

check_sql_stop $0 $S2P
./kill_m.sh
./run.sh

fail_if_zero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
check_master $0 $S2P $MP
check_count $0 $S2P 99001
./check $0 100001 "h=127.0.0.1,P=$S1P  h=127.0.0.1,P=$S3P h=127.0.0.1,P=$S4P"


