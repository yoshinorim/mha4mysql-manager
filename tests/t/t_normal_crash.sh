. ./init.sh
masterha_secondary_check  -s 127.0.0.1 --master_host=127.0.0.1 --master_ip=127.0.0.1 --master_port=10000 > /dev/null
fail_if_nonN $0 $? 3
./kill_m.sh
masterha_secondary_check  -s 127.0.0.1 --master_host=127.0.0.1 --master_ip=127.0.0.1 --master_port=10000 > /dev/null
fail_if_nonzero $0 $?
./run.sh
fail_if_nonzero $0 $?
mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 2
