. ./init.sh
mysql $S2 -e "set global relay_log_purge=0"
mysql $S1 -e "stop slave io_thread"
mysql $M test -e "flush logs"
mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"
mysql $M test -e "insert into t1 values(3, 300, 'ROLLBACK')"
mysql $M test -e "insert into t1 values(4, 400, \"Test\nROLLBACK\")"
mysql $M test -e "insert into t1 values(5, 500, 'bbbbb')"

./kill_m.sh
./run.sh
mysql $S2 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 6
