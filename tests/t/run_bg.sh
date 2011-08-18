#!/bin/sh
rm -f /var/tmp/*127.0.0.1_*.binlog
rm -f /var/tmp/*127.0.0.1_*.log
rm -f /var/tmp/mha_test*
masterha_manager --conf=$CONF --log_output=manager.log $@ < /dev/null > manager.log 2>&1 &
