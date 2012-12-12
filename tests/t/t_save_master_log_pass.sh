. ./init.sh
mysql $S1 -e "stop slave io_thread"
mysql $S2 -e "stop slave io_thread"
mysql $S3 -e "stop slave io_thread"
mysql $S4 -e "stop slave io_thread"

mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(3, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(4, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(5, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(6, 200, 'aaaaaa')"

./kill_m.sh
./run.sh --conf=mha_test_pass.cnf
fail_if_nonzero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"

./check $0 7
