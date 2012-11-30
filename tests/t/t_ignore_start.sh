. ./init.sh

./kill_m.sh
./stop_s4.sh
./run.sh --conf=$CONF_IGNORE --ignore_fail_on_start
fail_if_nonN $0 $? 10

echo "$0 [Pass]"
