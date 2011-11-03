. ./init.sh
masterha_manager --conf=mha_test_connect.cnf --log_output=manager.log > manager.log 2>&1 &
manager1_pid=$!
wait_until_manager_start $0 --conf=mha_test_connect.cnf

masterha_manager --conf=mha_test_connect.cnf --log_output=manager.log --skip_ssh_check >> manager.log 2>&1 &
manager2_pid=$!
sleep 30

./waitpid $manager1_pid
rc1=$?

./waitpid $manager2_pid
rc2=$?

masterha_stop --conf=mha_test_connect.cnf >> manager.log 2>&1

if [ $rc1 -ne 10 ] && [ $rc2 -ne 10 ]; then
  echo "$0 [Fail (both stopped)]"
  exit 1
fi

if [ $rc1 -eq 10 ] && [ $rc2 -eq 10 ]; then
  echo "$0 [Fail (both running)]"
  exit 1
fi

echo "$0 [Pass]"

