#!/usr/bin/env perl

#  Copyright (C) 2011 DeNA Co.,Ltd.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#  Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

package MHA::ManagerConst;

use strict;
use warnings FATAL => 'all';

use MHA::NodeConst;

our $VERSION          = '0.57';
our $NODE_MIN_VERSION = '0.54';

our @ALIVE_ERROR_CODES = (
  1040,    # ER_CON_COUNT_ERROR
  1042,    # ER_BAD_HOST_ERROR
  1043,    # ER_HANDSHAKE_ERROR
  1044,    # ER_DBACCESS_DENIED_ERROR
  1045,    # ER_ACCESS_DENIED_ERROR
  1129,    # ER_HOST_IS_BLOCKED
  1130,    # ER_HOST_NOT_PRIVILEGED
  1203,    # ER_TOO_MANY_USER_CONNECTIONS
  1226,    # ER_USER_LIMIT_REACHED
  1251,    # ER_NOT_SUPPORTED_AUTH_MODE
  1275,    # ER_SERVER_IS_IN_SECURE_AUTH_MODE
);

our $MYSQL_UNKNOWN_TID = 1094;

our $MASTER_DEAD_RC = 20;
our $MYSQL_DEAD_RC  = 10;

# Manager status
our $ST_RUNNING              = 0;
our $ST_NOT_RUNNING          = 2;
our $ST_PARTIALLY_RUNNING    = 3;
our $ST_INITIALIZING_MONITOR = 10;
our $ST_PING_FAILING         = 20;
our $ST_PING_FAILED          = 21;
our $ST_RETRYING_MONITOR     = 30;
our $ST_CONFIG_ERROR         = 31;
our $ST_TIMESTAMP_OLD        = 32;
our $ST_FAILOVER_RUNNING     = 50;
our $ST_FAILOVER_ERROR       = 51;

our $ST_RUNNING_S           = "$ST_RUNNING:PING_OK";
our $ST_NOT_RUNNING_S       = "$ST_NOT_RUNNING:NOT_RUNNING";
our $ST_PARTIALLY_RUNNING_S = "$ST_PARTIALLY_RUNNING:PARTIALLY_RUNNING";
our $ST_INITIALIZING_MONITOR_S =
  "$ST_INITIALIZING_MONITOR:INITIALIZING_MONITOR";
our $ST_PING_FAILING_S     = "$ST_PING_FAILING:PING_FAILING";
our $ST_PING_FAILED_S      = "$ST_PING_FAILED:PING_FAILED";
our $ST_RETRYING_MONITOR_S = "$ST_RETRYING_MONITOR:RETRYING_MONITOR";
our $ST_CONFIG_ERROR_S     = "$ST_CONFIG_ERROR:CONFIG_ERROR";
our $ST_TIMESTAMP_OLD_S    = "$ST_TIMESTAMP_OLD:TIMESTAMP_OLD";
our $ST_FAILOVER_RUNNING_S = "$ST_FAILOVER_RUNNING:FAILOVER_RUNNING";
our $ST_FAILOVER_ERROR_S   = "$ST_FAILOVER_ERROR:FAILOVER_ERROR";

our $USE_SSH_OPTIONS = 0;
our $SSH_OPT_ALIVE   = $MHA::NodeConst::SSH_OPT_ALIVE;
our $SSH_OPT_CHECK =
"-o StrictHostKeyChecking=no -o PasswordAuthentication=no -o BatchMode=yes -o ConnectTimeout=VAR_CONNECT_TIMEOUT";
our $SSH_OPT_ALIVE_DEFAULT = $SSH_OPT_ALIVE;
our $SSH_OPT_CHECK_DEFAULT = $SSH_OPT_CHECK;

our $PING_TYPE_CONNECT = "CONNECT";
our $PING_TYPE_SELECT  = "SELECT";
our $PING_TYPE_INSERT  = "INSERT";

our $DEFAULT_GLOBAL_CONF = "/etc/masterha_default.cnf";

our $log_fmt = sub {
  my %args = @_;
  my $msg  = $args{message};
  $msg = "" unless ($msg);
  chomp $msg;
  if ( $args{level} eq "error" ) {
    my ( $ln, $script ) = ( caller(4) )[ 2, 1 ];
    $script =~ s/.*:://;
    return sprintf( "[%s][%s, ln%d] %s\n", $args{level}, $script, $ln, $msg );
  }
  return sprintf( "[%s] %s\n", $args{level}, $msg );
};

our $add_timestamp = sub {
  my %p = @_;
  sprintf "%s - %s", scalar(localtime), $p{message};
};

1;
