. ./init.sh

./stop_s1.sh
./start_s1.sh --binlog-do-db=aaa
masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?

./run.sh
fail_if_zero $0 $?

./stop_s1.sh
./start_s1.sh --replicate-ignore-db=aaa
masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?

./run.sh
fail_if_zero $0 $?

./stop_s1.sh
./start_s1.sh --replicate-do-table=aaa
masterha_check_repl --conf=$CONF > /dev/null 2>&1
fail_if_zero $0 $?

./run.sh
fail_if_zero $0 $?


echo "$0 [Pass]"

