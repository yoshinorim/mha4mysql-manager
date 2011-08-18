. ./init.sh
mysql $S3 -e "set global relay_log_purge=0"
mysql $S1 -e "stop slave io_thread"
mysql $M test -e "flush logs"
mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"

./kill_m.sh
./run.sh
mysql $S2 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 3
