. ./init.sh

mysql $S4 -e "set global relay_log_purge=0"
mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"
mysql $S1 -e "stop slave io_thread"
mysql $M test -e "flush logs"
mysql $M test -e "insert into t1 values(3, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(4, 200, 'aaaaaa')"

mysql $S2 -e "stop slave io_thread"

mysql $M test -e "insert into t1 values(5, 200, 'aaaaaa')"

./kill_m.sh
./run.sh --conf=$CONF_LATEST
fail_if_nonzero $0 $?

mysql $S2 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 6
