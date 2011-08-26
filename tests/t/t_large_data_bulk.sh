. ./init.sh

mysql $S1 test -e "set global relay_log_purge=0"
mysql $S2 test -e "set global relay_log_purge=0"
mysql $S3 test -e "set global relay_log_purge=0"
mysql $S4 test -e "set global relay_log_purge=0"

mysql $S3 test -e "flush logs"
mysql $S3 test -e "flush logs"


perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 2 1000 0

mysql $S1 test -e "stop slave io_thread"

perl bulk_tran_insert.pl $MP $MYSQL_USER $MYSQL_PWD 1001 100000 1000
mysql $M test -e "update t1 set value = value+100 where id between 1 and 10000"
mysql $M test -e "update t1 set value = value+200 where id between 10001 and 20000"
mysql $M test -e "update t1 set value = value+300 where id between 20001 and 30000"
mysql $M test -e "update t1 set value = value+400 where id between 30001 and 40000"

sleep 1

./kill_m.sh
./run.sh

fail_if_nonzero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 100001

