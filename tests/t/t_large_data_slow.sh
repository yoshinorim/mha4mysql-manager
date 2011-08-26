. ./init.sh

# Fast position search should fail

mysql $S1 test -e "set global relay_log_purge=0"
mysql $S2 test -e "set global relay_log_purge=0"
mysql $S3 test -e "set global relay_log_purge=0"
mysql $S4 test -e "set global relay_log_purge=0"
mysql $S1 test -e "set global max_relay_log_size=65536"
mysql $S2 test -e "set global max_relay_log_size=65536"
mysql $S3 test -e "set global max_relay_log_size=65536"
mysql $S4 test -e "set global max_relay_log_size=65536"


perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 2 1000 0

mysql $S1 test -e "stop slave io_thread"

perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 1001 100000 1

sleep 1

./kill_m.sh
./run.sh

fail_if_nonzero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 100001

