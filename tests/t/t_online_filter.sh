. ./init.sh

sleep 1
./stop_m.sh
./start_m.sh --replicate-do-table=aaa
mysql $S1 -e "stop slave io_thread;start slave io_thread"
mysql $S2 -e "stop slave io_thread;start slave io_thread"
mysql $S3 -e "stop slave io_thread;start slave io_thread"
mysql $S4 -e "stop slave io_thread;start slave io_thread"
sleep 5

masterha_master_switch --master_state=alive --interactive=0 --conf=$CONF --new_master_host=127.0.0.1 --new_master_port=$S1P --orig_master_is_new_slave > switch.log 2>&1
fail_if_zero $0 $?

sleep 1
./stop_m.sh
./start_m.sh
mysql $S1 -e "stop slave io_thread;start slave io_thread"
mysql $S2 -e "stop slave io_thread;start slave io_thread"
mysql $S3 -e "stop slave io_thread;start slave io_thread"
mysql $S4 -e "stop slave io_thread;start slave io_thread"
sleep 5

masterha_master_switch --master_state=alive --interactive=0 --conf=$CONF --new_master_host=127.0.0.1 --new_master_port=$S1P > switch.log 2>&1
fail_if_nonzero $0 $?
mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 2
