. ./init.sh
skip_if_gtid $0

mysql $S1 -e "SET GLOBAL read_only=1"
./kill_m.sh
./run.sh --skip_change_master --skip_disable_read_only
fail_if_nonzero $0 $?
is_read_only $0 $S1P
check_master $0 $S1P $MP
check_master $0 $S2P $MP
check_master $0 $S3P $MP
check_master $0 $S4P $MP
echo "$0 [Pass]"
