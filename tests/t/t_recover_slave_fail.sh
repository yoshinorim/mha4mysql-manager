. ./init.sh
skip_if_gtid $0

mysql $S3 -e "stop slave io_thread"

mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(3, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(4, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(5, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(6, 200, 'aaaaaa')"

mysql $S3 test -e "insert into t1 values(4, 200, 'aaaaaa')"

./kill_m.sh
./run.sh

fail_if_zero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
check_master $0 $S3P $MP

check_count $0 $S3P 4
./check $0 7 "h=127.0.0.1,P=$S1P  h=127.0.0.1,P=$S2P h=127.0.0.1,P=$S4P"
