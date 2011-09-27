#!/bin/sh

./stop_s1.sh
./stop_s2.sh
rm -rf /var/tmp/mha_relay_test_dir
mkdir -p /var/tmp/mha_relay_test_dir
./start_s1.sh --relay_log_info=/var/tmp/mha_relay_test_dir/relay_log.info
./start_s2.sh --relay_log_info=/var/tmp/mha_relay_test_dir/relay_log2.info


