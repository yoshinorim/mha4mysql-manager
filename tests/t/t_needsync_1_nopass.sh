. ./init.sh
export MYSQL_PWD=""
masterha_check_repl --conf=mha_test_nopass.cnf > manager.log 2>&1
fail_if_nonzero $0 $?

export MYSQL_PWD=msandbox
mysql $S1 -e "stop slave io_thread"
mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"
wait_until_count $0 $S2P 2
wait_until_count $0 $S3P 2
wait_until_count $0 $S4P 2

./kill_m.sh
export MYSQL_PWD=""
./run.sh --conf=mha_test_nopass.cnf
export MYSQL_PWD=msandbox
mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 3
