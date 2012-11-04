. ./init.sh
mysql $S1 -e "stop slave io_thread"
mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"

./kill_m.sh
./run.sh --conf=mha_test_ssh.cnf
mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 3
