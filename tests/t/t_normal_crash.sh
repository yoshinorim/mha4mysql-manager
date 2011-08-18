. ./init.sh
./kill_m.sh
./run.sh
fail_if_nonzero $0 $?
mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 2
