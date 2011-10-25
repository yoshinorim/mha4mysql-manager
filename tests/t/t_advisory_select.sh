. ./init.sh

./run_bg.sh --conf=mha_test_select.cnf &
wait_until_manager_start $0 --conf=mha_test_select.cnf
masterha_manager --conf=mha_test_select.cnf > manager.log 2>&1
RC=$?
masterha_stop --conf=mha_test_select.cnf >> manager.log 2>&1
fail_if_zero $0 $RC

echo "$0 [Pass]"

