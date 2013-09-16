. ./init.sh

perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 2 1000 0

mysql $S3 test -e "stop slave io_thread"
mysql $S4 test -e "stop slave io_thread"

perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 1001 2700 0
mysql $S1 test -e "stop slave io_thread"
mysql $S2 test -e "stop slave io_thread"
perl insert.pl $MP $MYSQL_USER $MYSQL_PWD 2701 3000 0

mysql $M test -e "insert into t1 values(99999, 300, 'bbbaaaaaaa');"

./run_bg.sh --conf=$CONF_BINLOG
wait_until_manager_start $0 --conf=$CONF_BINLOG
./kill_m.sh
mkdir -p /tmp/mha_test
rm -rf /tmp/mha_test/*
cp -p $MASTER_DATA_DIR/mysql-bin.* /tmp/mha_test/
sleep 20

mysql $S1 test -e "insert into t1 values(10000003, 300, 'bbbaaaaaaa');"
check_master $0 $S3P $S1P
check_master $0 $S4P $S1P

./check $0 3002 "h=127.0.0.1,P=$S1P  h=127.0.0.1,P=$S2P  h=127.0.0.1,P=$S3P  h=127.0.0.1,P=$S4P"

