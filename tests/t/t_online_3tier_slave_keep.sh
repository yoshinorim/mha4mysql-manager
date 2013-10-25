. ./init.sh

mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"

sleep 1
./stop_m.sh
./start_m.sh --log-slave-updates
./stop_s1.sh
./start_s1.sh --log-slave-updates
is_gtid_supported
if test $? = 0
then
mysql $S1 -e "reset master"
fi
mysql $S1 -e "stop slave io_thread;start slave io_thread"
mysql $S2 -e "stop slave io_thread;start slave io_thread"
mysql $S3 -e "stop slave io_thread;start slave io_thread"
mysql $S4 -e "stop slave io_thread;start slave io_thread"
sleep 5

mysql $S2 -e "stop slave"
is_gtid_supported
if test $? = 1
then
mysql $S2 -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_auto_position=1"
else
mysql $S2 -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_log_file='mysql-bin.000001', master_log_pos=4"
fi
mysql $S2 -e "start slave"

check_master $0 $S2P $S1P

mysql $M test -e "insert into t1 values(3, 300, 'aaaaaa')"

masterha_master_switch --master_state=alive --interactive=0 --conf=mha_test_mm_online.cnf --new_master_host=127.0.0.1 --new_master_port=$S1P --orig_master_is_new_slave > switch.log 2>&1
fail_if_nonzero $0 $?

check_master $0 $S2P $S1P
check_master $0 $MP $S1P
check_master $0 $S3P $MP
check_master $0 $S4P $MP

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
check_count $0 $MP 4
./check $0 4
