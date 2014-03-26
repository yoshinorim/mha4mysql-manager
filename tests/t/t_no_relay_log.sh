. ./init.sh
skip_if_gtid $0


# Needs to apply lots of relay log files
# Relay log files switched many times while master binary log was not switched because max_relay_log_size is so small. In this case, each relay log does not contain a Rotate Event. This test case is to check recover works without rotate event.

mysql $S1 -e "set global max_relay_log_size=8192"
mysql $S2 -e "set global max_relay_log_size=8192"
mysql $S3 -e "set global max_relay_log_size=8192"
mysql $S4 -e "set global max_relay_log_size=8192"

perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 2 1000 0

sleep 2

mysql $S1 test -e "stop slave io_thread"

perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 1001 3000 0

sleep 1

./kill_m.sh
./run.sh

fail_if_zero $0 $?

check_master $0 $S1P $MP
check_master $0 $S2P $MP
check_master $0 $S3P $MP
check_master $0 $S4P $MP

echo "$0 [Pass]"
