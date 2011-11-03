. ./init.sh

masterha_master_switch --master_state=alive --interactive=0 --conf=mha_test_online.cnf --new_master_host=127.0.0.1 --new_master_port=$S1P --orig_master_is_new_slave --check_only > switch.log 2>&1
fail_if_nonzero $0 $?
check_master $0 $S1P $MP

mysql $M -e "create database if not exists mysqlslap"
sleep 1
mysqlslap --host=127.0.0.1 --port=$S1P --concurrency=15 --query="select sleep(100)" > /dev/null 2>&1 &
pid=$!

sleep 15
masterha_master_switch --master_state=alive --interactive=0 --conf=mha_test_online.cnf --new_master_host=127.0.0.1 --new_master_port=$S1P --orig_master_is_new_slave > switch.log 2>&1
rc=$?
kill $pid
wait $pid 2> /dev/null
fail_if_zero $0 $rc
sleep 10

mysql $M test -e "lock tables t1 write; select sleep(100)" > /dev/null 2>&1 &
pid2=$!
sleep 1
mysql $M test -e "lock tables t1 write"  > /dev/null 2>&1 &
pid3=$!
sleep 2

masterha_master_switch --master_state=alive --interactive=0 --conf=mha_test_online.cnf --new_master_host=127.0.0.1 --new_master_port=$S1P --orig_master_is_new_slave --flush_tables=0 > switch.log 2>&1
rc=$?
kill $pid2
wait $pid2 2> /dev/null
kill $pid3
wait $pid3 2> /dev/null
fail_if_zero $0 $rc
sleep 5

masterha_master_switch --master_state=alive --interactive=0 --conf=mha_test_online.cnf --new_master_host=127.0.0.1 --new_master_port=$S1P --orig_master_is_new_slave > switch.log 2>&1
fail_if_nonzero $0 $?
mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
check_master $0 $MP $S1P
check_count $0 $MP 2
./check $0 2

