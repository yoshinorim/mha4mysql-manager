. ./init.sh
skip_if_gtid $0
mysql $S1 -e "stop slave io_thread"
mysql $M test -e "flush logs"
mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"

./kill_m.sh
./run.sh

fail_if_zero $0 $?

echo "$0 [Pass]"
