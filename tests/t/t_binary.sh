# Needs 5.6.3+ client to pass this test case
. ./init.sh

mysql $M test -e "create table binfile (bin_data blob) charset=latin1"
mysql $M test -e "drop table t1"
mysql $S2 -e "stop slave"

perl insert_binary.pl $MP $MYSQL_USER $MYSQL_PWD

./kill_m.sh
./run.sh
fail_if_nonzero $0 $?
./check $0 1
