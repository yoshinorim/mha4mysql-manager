. ./init.sh

./stop_s4.sh
./run.sh --conf=$CONF_IGNORE
fail_if_zero $0 $?

./kill_m.sh
./stop_s4.sh
./run.sh --conf=$CONF_IGNORE
fail_if_zero $0 $?

echo "$0 [Pass]"
