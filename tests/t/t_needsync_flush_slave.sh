. ./init.sh
mysql $S1 -e "stop slave io_thread"

for i in $S2P $S3P $S4P
do
mysql -h127.0.0.1 -P$i -e "set global relay_log_purge=0"
mysql -h127.0.0.1 -P$i test -e "flush logs"
mysql -h127.0.0.1 -P$i test -e "flush logs"
mysql -h127.0.0.1 -P$i test -e "flush logs"
mysql -h127.0.0.1 -P$i test -e "flush logs"
mysql -h127.0.0.1 -P$i test -e "flush logs"
done

mysql $M test -e "insert into t1 values(2, 200, 'aaaaaa')"

./kill_m.sh
./run.sh
mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
./check $0 3
