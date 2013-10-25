. ./init.sh

is_gtid_supported
if test $? = 1
then
mysql $M -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_auto_position=1; start slave"
else
mysql $M -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_log_file='mysql-bin.000001', master_log_pos=4; start slave"
fi

./stop_s1.sh
./start_s1.sh --log-slave-updates
sleep 5;

mysql $M -e "set global read_only=0"
mysql $S1 -e "set global read_only=1"

masterha_master_switch --master_state=alive --interactive=0 --conf=mha_test_reset.cnf --new_master_host=127.0.0.1 --new_master_port=$S1P --orig_master_is_new_slave > switch.log 2>&1
fail_if_nonzero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
check_sql_stop $0 $S1P
mysql $S1 -e "start slave"
check_sql_yes $0 $S1P
mysql $M -e "set global read_only=0"
mysql $M test -e "insert into t1 values(10000004, 300, 'bbbaaaaaaa');"

./check $0 3 "h=127.0.0.1,P=$MP h=127.0.0.1,P=$S2P h=127.0.0.1,P=$S3P h=127.0.0.1,P=$S4P"

