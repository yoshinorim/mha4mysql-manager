kill -9 `ps -ef | grep rsandbox_$VERSION_DIR  | grep master | grep mysqld_safe | awk '{print $2}'`
kill -9 `cat $SANDBOX_HOME/rsandbox_$VERSION_DIR/master/data/mysql_sandbox10000.pid`
