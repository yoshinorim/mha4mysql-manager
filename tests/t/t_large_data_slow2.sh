. ./init.sh
# new master(S1)'s sql thread is stopped too long time so S1 can't be a new master
# It takes very long time on virtual machine

mysql $S1 test -e "set global relay_log_purge=0"
mysql $S2 test -e "set global relay_log_purge=0"
mysql $S3 test -e "set global relay_log_purge=0"
mysql $S4 test -e "set global relay_log_purge=0"

mysql $S3 test -e "flush logs"
mysql $S3 test -e "flush logs"


perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 2 1000 0

mysql $S1 test -e "stop slave io_thread"

# very long transaction
perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 1001 1000000 0

sleep 1

./kill_m.sh
./run.sh

fail_if_nonzero $0 $?

mysql $S2 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 1000001

