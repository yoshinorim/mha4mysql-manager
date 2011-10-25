. ./init.sh
rm -f /var/tmp/*127.0.0.1_*.binlog
rm -f /var/tmp/*127.0.0.1_*.log
rm -f /var/tmp/mha_test*
masterha_manager --conf=$CONF --log_output=manager.log > manager.log 2>&1 &
manager1_pid=$!
wait_until_manager_start $0

masterha_manager --conf=$CONF --log_output=manager.log --skip_ssh_check >> manager.log 2>&1 &
manager2_pid=$!
sleep 30

./waitpid $manager1_pid
rc1=$?

./waitpid $manager2_pid
rc2=$?

if [ $rc1 -ne 10 ] && [ $rc2 -ne 10 ]; then
  echo "$0 [Fail (both stopped)]"
fi

if [ $rc1 -eq 10 ] && [ $rc2 -eq 10 ]; then
  echo "$0 [Fail (both running)]"
fi

masterha_stop --conf=$CONF >> manager.log 2>&1

echo "$0 [Pass]"

