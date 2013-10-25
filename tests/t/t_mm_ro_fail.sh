. ./init.sh

is_gtid_supported
if test $? = 1
then
mysql $M -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_auto_position=1; start slave"
else
mysql $M -e "change master to master_host='127.0.0.1', master_port=$S1P, master_user='rsandbox', master_password='rsandbox', master_log_file='mysql-bin.000001', master_log_pos=4; start slave"
fi

masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh 
fail_if_zero $0 $?

mysql $S1 -e "set global read_only=1"
mysql $M -e "set global read_only=1"

masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh 
fail_if_zero $0 $?

echo "$0 [Pass]"
