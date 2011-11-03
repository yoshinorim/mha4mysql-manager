. ./init.sh

./run_bg.sh  &
wait_until_manager_start $0
masterha_manager --conf=$CONF >> manager.log 2>&1
RC=$?
sleep 5
masterha_stop --conf=$CONF >> manager.log 2>&1
fail_if_zero $0 $RC

echo "$0 [Pass]"

