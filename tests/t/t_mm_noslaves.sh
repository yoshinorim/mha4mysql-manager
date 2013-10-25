. ./init.sh

is_gtid_supported
if test $? = 1
then
mysql $M -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_auto_position=1; start slave"
else
mysql $M -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_log_file='mysql-bin.000001', master_log_pos=4; start slave"
fi

sleep 1

masterha_check_repl --conf=mha_test_mm.cnf > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh --conf=mha_test_mm.cnf
fail_if_zero $0 $?

mysql $S1 -e "set global read_only=1"
mysql $M -e "set global read_only=1"

masterha_check_repl --conf=mha_test_mm.cnf > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh --conf=mha_test_mm.cnf
fail_if_zero $0 $?

mysql $M -e "set global read_only=0"

masterha_check_repl --conf=mha_test_mm.cnf > /dev/null 2>&1
fail_if_nonzero $0 $?

mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(3, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(4, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(5, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(6, 200, 'aaaaaa')"

./kill_m.sh
./run.sh --conf=mha_test_mm.cnf
fail_if_nonzero $0 $?

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"

./check $0 7 "h=127.0.0.1,P=$S1P"

