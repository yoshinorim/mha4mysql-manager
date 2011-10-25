rm -f $SANDBOX_HOME/rsandbox_$VERSION_DIR/master/data/*.pid
$SANDBOX_HOME/rsandbox_$VERSION_DIR/master/start $@ > /dev/null
