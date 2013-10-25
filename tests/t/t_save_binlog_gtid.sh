. ./init.sh
skip_if_not_gtid $0
mysql $S1 -e "stop slave io_thread"
mysql $S2 -e "stop slave io_thread"
mysql $S3 -e "stop slave io_thread"
mysql $S4 -e "stop slave io_thread"

mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(3, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(4, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(5, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(6, 200, 'aaaaaa')"

./kill_m.sh
./run.sh --conf=mha_test_gtid_fail1.cnf
fail_if_zero $0 $?

rm -f /var/tmp/mha_test*
masterha_master_switch  --master_state=dead --interactive=0 --dead_master_host=127.0.0.1 --dead_master_ip=127.0.0.1 --dead_master_port=$MP  --conf=mha_test_gtid_fail1.cnf > /dev/null 2>&1
fail_if_zero $0 $?

rm -f /var/tmp/mha_test*
masterha_master_switch  --master_state=dead --interactive=0 --dead_master_host=127.0.0.1 --dead_master_ip=127.0.0.1 --dead_master_port=$MP  --conf=mha_test_gtid_fail2.cnf > /dev/null 2>&1
fail_if_zero $0 $?

./run.sh --conf=mha_test_gtid_ok.cnf
masterha_master_switch  --master_state=dead --interactive=0 --dead_master_host=127.0.0.1 --dead_master_ip=127.0.0.1 --dead_master_port=$MP  --conf=mha_test_gtid_ok.cnf > /dev/null 2>&1
fail_if_nonzero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"

./check $0 7
