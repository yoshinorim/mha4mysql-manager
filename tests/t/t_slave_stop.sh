. ./init.sh
./stop_s1.sh
./run.sh
fail_if_zero $0 $?

./start_s1.sh
./kill_m.sh
./run.sh
mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 2

