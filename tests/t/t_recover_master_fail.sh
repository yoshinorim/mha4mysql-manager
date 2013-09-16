. ./init.sh
mysql $S1 -e "stop slave io_thread"
mysql $S3 -e "stop slave io_thread"

mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(3, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(4, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(5, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(6, 200, 'aaaaaa')"

mysql $S1 test -e "insert into t1 values(4, 200, 'aaaaaa')"

./kill_m.sh
./run.sh

fail_if_zero $0 $?
#check_master $0 $S1P $MP
check_master $0 $S2P $MP
check_master $0 $S3P $MP
check_master $0 $S4P $MP

check_count $0 $S1P 4
check_count $0 $S2P 6
check_count $0 $S3P 1

echo "$0 [Pass]"
