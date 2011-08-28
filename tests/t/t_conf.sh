. ./init.sh

masterha_check_repl --conf=mha_test_err1.cnf > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh --conf=mha_test_err1.cnf
fail_if_zero $0 $?

masterha_check_repl --conf=mha_test_err2.cnf > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh --conf=mha_test_err2.cnf
fail_if_zero $0 $?

echo "$0 [Pass]"
