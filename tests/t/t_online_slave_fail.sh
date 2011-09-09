. ./init.sh

masterha_master_switch --master_state=alive --interactive=0 --conf=$CONF --new_master_host=127.0.0.1 --new_master_port=$S1P --orig_master_is_new_slave > switch.log 2>&1
fail_if_zero $0 $?
mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
check_count $0 $MP 1
./check $0 2

