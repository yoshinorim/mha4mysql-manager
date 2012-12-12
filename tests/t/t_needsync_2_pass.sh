. ./init.sh
mysql $S4 test -e "set global relay_log_purge=0"

perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 2 1000 0

sleep 2

mysql $S1 test -e "stop slave io_thread"

perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 1001 3000 0

sleep 1

./kill_m.sh
./run.sh  --conf=mha_test_pass.cnf

fail_if_nonzero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 3001

