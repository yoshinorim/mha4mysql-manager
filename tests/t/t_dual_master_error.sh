. ./init.sh

mysql $S1 -e "stop slave; reset slave;"

masterha_check_repl --conf=mha_test_mm.cnf > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh --conf=mha_test_mm.cnf
fail_if_zero $0 $?

masterha_check_repl --conf=mha_test.cnf > /dev/null 2>&1
fail_if_zero $0 $?
./run.sh --conf=mha_test.cnf
fail_if_zero $0 $?

echo "$0 [Pass]"
