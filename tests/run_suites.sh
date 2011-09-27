#!/bin/sh
cd t
echo "5.5.16 row"
./run_tests  --mysql_version=5.5.16 --use_row_format
echo "5.5.16 stmt"
./run_tests  --mysql_version=5.5.16 
echo "5.5.16 stmt relay log info"
./run_tests  --mysql_version=5.5.16  --init_script=change_relay_log_info.sh --tests=need*
./run_tests  --mysql_version=5.5.16  --init_script=change_relay_log_info.sh --tests=large*
echo "5.5.15 stmt"
./run_tests  --mysql_version=5.5.15 
echo "5.1.58 row"
./run_tests  --mysql_version=5.1.58 --use_row_format
echo "5.1.58 stmt"
./run_tests  --mysql_version=5.1.58
echo "5.1.58 stmt relay log info"
./run_tests  --mysql_version=5.1.58  --init_script=change_relay_log_info.sh --tests=need*
./run_tests  --mysql_version=5.1.58  --init_script=change_relay_log_info.sh --tests=large*
echo "5.0.91"
./run_tests  --mysql_version=5.0.91
echo "5.0.45"
./run_tests  --mysql_version=5.0.45
