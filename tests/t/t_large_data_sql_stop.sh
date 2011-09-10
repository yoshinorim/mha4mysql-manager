. ./init.sh

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

fail_if_nonzero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 100001

