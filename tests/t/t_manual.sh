. ./init.sh

mysql $M test -e "insert into t1 values (2, 200, 'aaaaa')"

masterha_master_switch  --master_state=dead --interactive=0 --dead_master_host=127.0.0.1 --dead_master_ip=127.0.0.1 --dead_master_port=$MP --ssh_reachable=1 --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?

./kill_m.sh
./stop_s1.sh

masterha_master_switch  --master_state=dead --interactive=0 --dead_master_host=127.0.0.1 --dead_master_ip=127.0.0.1 --dead_master_port=$MP --ssh_reachable=1 --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?

./start_s1.sh

masterha_master_switch  --master_state=dead --interactive=0 --dead_master_host=127.0.0.1 --dead_master_ip=127.0.0.1 --dead_master_port=$MP --ssh_reachable=1 --conf=$CONF > /dev/null 2>&1
fail_if_nonzero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 3
