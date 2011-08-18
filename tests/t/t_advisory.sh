. ./init.sh

./run_bg.sh &
sleep 10
masterha_manager --conf=$CONF > manager.log 2>&1
RC=$?
masterha_stop --conf=$CONF >> manager.log 2>&1
fail_if_zero $0 $RC

echo "$0 [Pass]"

