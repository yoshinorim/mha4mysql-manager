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

package MHA::DBHelper;

use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use MHA::SlaveUtil;
use MHA::ManagerConst;
use Carp qw(croak);
use DBI;
use Data::Dumper;
use Log::Dispatch;

use constant Status => "Status";
use constant Errstr => "Errstr";

#show master status output
use constant File              => "File";
use constant Position          => "Position";
use constant Binlog_Do_DB      => "Binlog_Do_DB";
use constant Binlog_Ignore_DB  => "Binlog_Ignore_DB";
use constant Executed_Gtid_Set => "Executed_Gtid_Set";

#show slave status output
use constant Slave_IO_State              => "Slave_IO_State";
use constant Slave_SQL_Running           => "Slave_SQL_Running";
use constant Slave_IO_Running            => "Slave_IO_Running";
use constant Master_Log_File             => "Master_Log_File";
use constant Master_Host                 => "Master_Host";
use constant Master_User                 => "Master_User";
use constant Master_Port                 => "Master_Port";
use constant Replicate_Do_DB             => "Replicate_Do_DB";
use constant Replicate_Ignore_DB         => "Replicate_Ignore_DB";
use constant Replicate_Do_Table          => "Replicate_Do_Table";
use constant Replicate_Ignore_Table      => "Replicate_Ignore_Table";
use constant Replicate_Wild_Do_Table     => "Replicate_Wild_Do_Table";
use constant Replicate_Wild_Ignore_Table => "Replicate_Wild_Ignore_Table";
use constant Read_Master_Log_Pos         => "Read_Master_Log_Pos";
use constant Relay_Master_Log_File       => "Relay_Master_Log_File";
use constant Exec_Master_Log_Pos         => "Exec_Master_Log_Pos";
use constant Relay_Log_File              => "Relay_Log_File";
use constant Relay_Log_Pos               => "Relay_Log_Pos";
use constant Seconds_Behind_Master       => "Seconds_Behind_Master";
use constant Last_Errno                  => "Last_Errno";
use constant Last_Error                  => "Last_Error";
use constant Retrieved_Gtid_Set          => "Retrieved_Gtid_Set";
use constant Auto_Position               => "Auto_Position";

use constant Set_Long_Wait_Timeout_SQL => "SET wait_timeout=86400";
use constant Show_One_Variable_SQL     => "SHOW GLOBAL VARIABLES LIKE ?";
use constant Change_Master_SQL =>
"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_LOG_FILE='%s', MASTER_LOG_POS=%d";
use constant Change_Master_NoPass_SQL =>
"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_LOG_FILE='%s', MASTER_LOG_POS=%d";
use constant Change_Master_Gtid_SQL =>
"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_AUTO_POSITION=1";
use constant Change_Master_Gtid_NoPass_SQL =>
"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_AUTO_POSITION=1";
use constant Reset_Slave_Master_Host_SQL => "RESET SLAVE /*!50516 ALL */";
use constant Reset_Slave_SQL             => "RESET SLAVE";
use constant Change_Master_Clear_SQL     => "CHANGE MASTER TO MASTER_HOST=''";
use constant Show_Slave_Status_SQL       => "SHOW SLAVE STATUS";

# i_s.processlist was not supported in older versions
#use constant Show_Processlist_SQLThread_SQL=>"select * from information_schema.processlist where user='system user' and state like 'Has read all relay log%';";
use constant Show_Processlist_SQL   => "SHOW PROCESSLIST";
use constant Show_Master_Status_SQL => "SHOW MASTER STATUS";
use constant Stop_IO_Thread_SQL     => "STOP SLAVE IO_THREAD";
use constant Start_IO_Thread_SQL    => "START SLAVE IO_THREAD";
use constant Start_Slave_SQL        => "START SLAVE";
use constant Stop_Slave_SQL         => "STOP SLAVE";
use constant Start_SQL_Thread_SQL   => "START SLAVE SQL_THREAD";
use constant Stop_SQL_Thread_SQL    => "STOP SLAVE SQL_THREAD";
use constant Get_Basedir_SQL        => "SELECT \@\@global.basedir AS Value";
use constant Get_Datadir_SQL        => "SELECT \@\@global.datadir AS Value";
use constant Get_Num_Workers_SQL =>
  "SELECT \@\@global.slave_parallel_workers AS Value";
use constant Get_MaxAllowedPacket_SQL =>
  "SELECT \@\@global.max_allowed_packet AS Value";
use constant Set_MaxAllowedPacket1G_SQL =>
  "SET GLOBAL max_allowed_packet=1*1024*1024*1024";
use constant Set_MaxAllowedPacket_SQL => "SET GLOBAL max_allowed_packet=%d";
use constant Is_Readonly_SQL          => "SELECT \@\@global.read_only As Value";
use constant Has_Gtid_SQL             => "SELECT \@\@global.gtid_mode As Value";
use constant Get_ServerID_SQL         => "SELECT \@\@global.server_id As Value";
use constant Unset_Readonly_SQL       => "SET GLOBAL read_only=0";
use constant Set_Readonly_SQL         => "SET GLOBAL read_only=1";
use constant Unset_Log_Bin_Local_SQL  => "SET sql_log_bin=0";
use constant Set_Log_Bin_Local_SQL    => "SET sql_log_bin=1";
use constant Rename_User_SQL          => "RENAME USER '%s'\@'%%' TO '%s'\@'%%'";
use constant Master_Pos_Wait_NoTimeout_SQL =>
  "SELECT MASTER_POS_WAIT(?,?,0) AS Result";
use constant Gtid_Wait_NoTimeout_SQL =>
  "SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS(?,0) AS Result";
use constant Get_Connection_Id_SQL  => "SELECT CONNECTION_ID() AS Value";
use constant Flush_Tables_Nolog_SQL => "FLUSH NO_WRITE_TO_BINLOG TABLES";
use constant Flush_Tables_With_Read_Lock_SQL => "FLUSH TABLES WITH READ LOCK";
use constant Unlock_Tables_SQL               => "UNLOCK TABLES";
use constant Repl_User_SQL =>
  "SELECT Repl_slave_priv AS Value FROM mysql.user WHERE user = ?";
use constant Select_User_Regexp_SQL =>
"SELECT user, host, password FROM mysql.user WHERE user REGEXP ? AND host REGEXP ?";
use constant Set_Password_SQL             => "SET PASSWORD FOR ?\@? = ?";
use constant Old_Password_Length          => 16;
use constant Blocked_Empty_Password       => '?' x 41;
use constant Blocked_Old_Password_Head    => '~' x 25;
use constant Blocked_New_Password_Regexp  => qr/^[0-9a-fA-F]{40}\*$/o;
use constant Released_New_Password_Regexp => qr/^\*[0-9a-fA-F]{40}$/o;

sub new {
  my $class = shift;
  my $self  = {
    dsn           => undef,
    dbh           => undef,
    connection_id => undef,
    has_gtid      => undef,
    is_mariadb    => undef,
    @_,
  };
  return bless $self, $class;
}

sub get_connection_id($) {
  my $self = shift;
  my $sth  = $self->{dbh}->prepare(Get_Connection_Id_SQL);
  $sth->execute();
  my $href = $sth->fetchrow_hashref;
  return $href->{Value};
}

sub connect_util {
  my $host     = shift;
  my $port     = shift;
  my $user     = shift;
  my $password = shift;
  my $dsn      = "DBI:mysql:;host=$host;port=$port;mysql_connect_timeout=1";
  my $dbh      = DBI->connect( $dsn, $user, $password, { PrintError => 0 } );
  return $dbh;
}

sub check_connection_fast_util {
  my $host     = shift;
  my $port     = shift;
  my $user     = shift;
  my $password = shift;
  my $dbh      = connect_util( $host, $port, $user, $password );
  if ( defined($dbh) ) {
    $dbh->disconnect();
    return "1:Connection Succeeded";
  }
  else {
    my $mysql_err = DBI->err;
    if ( $mysql_err
      && grep ( $_ == $mysql_err, @MHA::ManagerConst::ALIVE_ERROR_CODES ) > 0 )
    {
      my $rc = $mysql_err;
      $rc .= "($DBI::errstr)" if ($DBI::errstr);
      return $rc;
    }
  }

  #server is dead
  return 0;
}

sub connect {
  my $self        = shift;
  my $host        = shift;
  my $port        = shift;
  my $user        = shift;
  my $password    = shift;
  my $raise_error = shift;
  my $max_retries = shift;
  $raise_error = 0 if ( !defined($raise_error) );
  $max_retries = 2 if ( !defined($max_retries) );

  $self->{dbh} = undef;
  unless ( $self->{dsn} ) {
    $self->{dsn} = "DBI:mysql:;host=$host;port=$port;mysql_connect_timeout=4";
  }
  my $defaults = {
    PrintError => 0,
    RaiseError => ( $raise_error ? 1 : 0 ),
  };
  while ( !$self->{dbh} && $max_retries-- ) {
    eval {
      $self->{dbh} = DBI->connect( $self->{dsn}, $user, $password, $defaults );
    };
    if ( !$self->{dbh} && $@ ) {
      croak $@ if ( !$max_retries );
    }
  }
  if ( $self->{dbh} ) {
    $self->{connection_id} = $self->get_connection_id();
    $self->{dbh}->{InactiveDestroy} = 1;
  }
  return $self->{dbh};
}

sub disconnect($) {
  my $self = shift;
  $self->{dbh}->disconnect() if ( $self->{dbh} );
}

sub get_variable {
  my $self  = shift;
  my $query = shift;
  my $sth   = $self->{dbh}->prepare($query);
  $sth->execute();
  my $href = $sth->fetchrow_hashref;
  return $href->{Value};
}

# display one value that are not supported by select @@..
sub show_variable($$) {
  my $self = shift;
  my $cond = shift;
  my $sth  = $self->{dbh}->prepare(Show_One_Variable_SQL);
  $sth->execute($cond);
  my $href = $sth->fetchrow_hashref;
  return $href->{Value};
}

sub has_repl_priv {
  my $self = shift;
  my $user = shift;
  my $sth  = $self->{dbh}->prepare(Repl_User_SQL);
  my $ret  = $sth->execute($user);
  if ( !defined($ret) ) {
    croak
"Got MySQL error when checking replication privilege. $DBI::err: $DBI::errstr query:"
      . Repl_User_SQL . "\n";
  }
  my $href  = $sth->fetchrow_hashref;
  my $value = $href->{Value};
  return 1 if ( defined($value) && $value eq "Y" );
  return 0;
}

sub is_binlog_enabled($) {
  my $self  = shift;
  my $value = $self->show_variable("log_bin");
  return 1 if ( defined($value) && $value eq "ON" );
  return 0;
}

sub is_read_only($) {
  my $self = shift;
  return $self->get_variable(Is_Readonly_SQL);
}

sub has_gtid($) {
  my $self  = shift;
  my $value = $self->get_variable(Has_Gtid_SQL);
  if ( defined($value) && $value eq "ON" ) {
    $self->{has_gtid} = 1;
    return 1;
  }
  return 0;
}

sub get_basedir($) {
  my $self = shift;
  return $self->get_variable(Get_Basedir_SQL);
}

sub get_datadir($) {
  my $self = shift;
  return $self->get_variable(Get_Datadir_SQL);
}

sub get_num_workers($) {
  my $self = shift;
  return $self->get_variable(Get_Num_Workers_SQL);
}

sub get_version($) {
  my $self  = shift;
  my $value = MHA::SlaveUtil::get_version( $self->{dbh} );
  if ( $value =~ /MariaDB/ ) {
    $self->{is_mariadb} = 1;
  }
  return $value;
}

sub is_relay_log_purge($) {
  my $self = shift;
  return MHA::SlaveUtil::is_relay_log_purge( $self->{dbh} );
}

sub disable_relay_log_purge($) {
  my $self = shift;
  return MHA::SlaveUtil::disable_relay_log_purge( $self->{dbh} );
}

sub get_relay_log_info_type {
  my ( $self, $mysql_version ) = @_;
  return MHA::SlaveUtil::get_relay_log_info_type( $self->{dbh},
    $mysql_version );
}

sub get_relay_log_info_path {
  my ( $self, $mysql_version ) = @_;
  return MHA::SlaveUtil::get_relay_log_info_path( $self->{dbh},
    $mysql_version );
}

sub get_server_id($) {
  my $self = shift;
  return $self->get_variable(Get_ServerID_SQL);
}

sub get_max_allowed_packet($) {
  my $self = shift;
  return $self->get_variable(Get_MaxAllowedPacket_SQL);
}

sub set_max_allowed_packet($$) {
  my $self  = shift;
  my $size  = shift;
  my $query = sprintf( Set_MaxAllowedPacket_SQL, $size );
  return $self->execute($query);
}

sub set_max_allowed_packet_1g($) {
  my $self = shift;
  return $self->execute(Set_MaxAllowedPacket1G_SQL);
}

sub show_master_status($) {
  my $self = shift;
  my ( $query, $sth, $href );
  my %values;
  $query = Show_Master_Status_SQL;
  $sth   = $self->{dbh}->prepare($query);
  my $ret = $sth->execute();
  return if ( !defined($ret) || $ret != 1 );

  $href = $sth->fetchrow_hashref;
  for my $key ( File, Position, Executed_Gtid_Set ) {
    $values{$key} = $href->{$key};
  }
  for my $filter_key ( Binlog_Do_DB, Binlog_Ignore_DB ) {
    $values{$filter_key} = uniq_and_sort( $href->{$filter_key} );
  }
  return (
    $values{File}, $values{Position}, $values{Binlog_Do_DB},
    $values{Binlog_Ignore_DB},
    $values{Executed_Gtid_Set}
  );

}

sub execute_update {
  my ( $self, $query, $expected_affected_rows, $bind_args ) = @_;
  my %status = ();
  my @params;
  if ( defined $bind_args ) {
    push @params, @$bind_args;
  }
  my $sth = $self->{dbh}->prepare($query);
  my $ret = $sth->execute(@params);
  if ( !defined($ret) || $ret != $expected_affected_rows ) {
    $status{Status} = -1;
    $status{Errstr} = $sth->errstr;
    return %status;
  }
  $status{Status} = $ret;
  return %status;
}

sub execute {
  my ( $self, $query, $bind_args ) = @_;
  my %status = $self->execute_update( $query, 0E0, $bind_args );
  if ( $status{Status} == 0 ) {
    return 0;
  }
  elsif ( $status{Errstr} ) {
    return $status{Errstr};
  }
  else {
    return 1;
  }
}

sub flush_tables_nolog($) {
  my $self = shift;
  return $self->execute(Flush_Tables_Nolog_SQL);
}

sub flush_tables_with_read_lock($) {
  my $self = shift;
  return $self->execute(Flush_Tables_With_Read_Lock_SQL);
}

sub unlock_tables($) {
  my $self = shift;
  return $self->execute(Unlock_Tables_SQL);
}

sub set_wait_timeout_util($$) {
  my $dbh     = shift;
  my $timeout = shift;
  my $sth     = $dbh->prepare( sprintf( "SET wait_timeout=%d", $timeout ) );
  my $ret     = $sth->execute();
  return 1 if ( !defined($ret) || $ret != 0E0 );
  return 0;
}

sub set_long_wait_timeout($) {
  my $self = shift;
  return $self->execute(Set_Long_Wait_Timeout_SQL);
}

sub reset_slave_master_host($) {
  my $self = shift;
  return $self->execute(Reset_Slave_Master_Host_SQL);
}

sub reset_slave_by_change_master($) {
  my $self = shift;
  return $self->execute(Change_Master_Clear_SQL);
}

sub change_master($$$$$$$) {
  my $self            = shift;
  my $master_host     = shift;
  my $master_port     = shift;
  my $master_log_file = shift;
  my $master_log_pos  = shift;
  my $master_user     = shift;
  my $master_password = shift;

  my $query;
  if ($master_password) {
    $query = sprintf( Change_Master_SQL,
      $master_host,     $master_port,     $master_user,
      $master_password, $master_log_file, $master_log_pos );
  }
  else {
    $query = sprintf(
      Change_Master_NoPass_SQL,
      $master_host,     $master_port, $master_user,
      $master_log_file, $master_log_pos
    );
  }
  return $self->execute($query);
}

sub change_master_gtid($$$$$) {
  my $self            = shift;
  my $master_host     = shift;
  my $master_port     = shift;
  my $master_user     = shift;
  my $master_password = shift;
  my $query;
  if ($master_password) {
    $query = sprintf( Change_Master_Gtid_SQL,
      $master_host, $master_port, $master_user, $master_password );
  }
  else {
    $query = sprintf( Change_Master_Gtid_NoPass_SQL,
      $master_host, $master_port, $master_user );
  }
  return $self->execute($query);
}

sub disable_log_bin_local($) {
  my $self = shift;
  return $self->execute(Unset_Log_Bin_Local_SQL);
}

sub enable_log_bin_local($) {
  my $self = shift;
  return $self->execute(Set_Log_Bin_Local_SQL);
}

sub enable_read_only($) {
  my $self = shift;
  if ( $self->is_read_only() eq "1" ) {
    return 0;
  }
  else {
    return $self->execute(Set_Readonly_SQL);
  }
}

sub disable_read_only($) {
  my $self = shift;
  if ( $self->is_read_only() eq "0" ) {
    return 0;
  }
  else {
    return $self->execute(Unset_Readonly_SQL);
  }
}

sub reset_slave($) {
  my $self = shift;
  return $self->execute(Reset_Slave_SQL);
}

sub start_io_thread($) {
  my $self = shift;
  return $self->execute(Start_IO_Thread_SQL);
}

sub stop_io_thread($) {
  my $self = shift;
  return $self->execute(Stop_IO_Thread_SQL);
}

sub start_slave() {
  my $self = shift;
  return $self->execute(Start_Slave_SQL);
}

sub start_sql_thread() {
  my $self = shift;
  return $self->execute(Start_SQL_Thread_SQL);
}

sub stop_sql_thread() {
  my $self = shift;
  return $self->execute(Stop_SQL_Thread_SQL);
}

sub stop_slave() {
  my $self = shift;
  return $self->execute(Stop_Slave_SQL);
}

sub uniq_and_sort {
  my $str = shift;
  my @array = split( /,/, $str );
  my %count;
  @array = grep( !$count{$_}++, @array );
  @array = sort @array;
  return join( ',', @array );
}

sub check_slave_status {
  my $self        = shift;
  my $allow_dummy = shift;
  my ( $query, $sth, $href );
  my %status = ();

  unless ( $self->{dbh} ) {
    $status{Status} = 1;
    $status{Errstr} = "Database Handle is not defined!";
    return %status;
  }

  $query = Show_Slave_Status_SQL;
  $sth   = $self->{dbh}->prepare($query);
  my $ret = $sth->execute();
  if ( !defined($ret) || $ret != 1 ) {

    # I am not a slave
    $status{Status} = 1;

    # unexpected error
    if ( defined( $sth->errstr ) ) {
      $status{Status} = 2;
      $status{Errstr} =
          "Got error when executing "
        . Show_Slave_Status_SQL . ". "
        . $sth->errstr;
    }
    return %status;
  }

  $status{Status} = 0;
  $href = $sth->fetchrow_hashref;

  for my $key (
    Slave_IO_State,        Master_Host,
    Master_Port,           Master_User,
    Slave_IO_Running,      Slave_SQL_Running,
    Master_Log_File,       Read_Master_Log_Pos,
    Relay_Master_Log_File, Last_Errno,
    Last_Error,            Exec_Master_Log_Pos,
    Relay_Log_File,        Relay_Log_Pos,
    Seconds_Behind_Master, Retrieved_Gtid_Set,
    Executed_Gtid_Set,     Auto_Position
    )
  {
    $status{$key} = $href->{$key};
  }

  if ( !$status{Master_Host}
    || !$status{Master_Log_File} )
  {
    unless ($allow_dummy) {

      # I am not a slave
      $status{Status} = 1;
      return %status;
    }
  }

  for my $filter_key ( Replicate_Do_DB, Replicate_Ignore_DB, Replicate_Do_Table,
    Replicate_Ignore_Table, Replicate_Wild_Do_Table,
    Replicate_Wild_Ignore_Table )
  {
    $status{$filter_key} = uniq_and_sort( $href->{$filter_key} );
  }

  return %status;
}

sub wait_until_relay_io_log_applied($$$) {
  my $self               = shift;
  my $log                = shift;
  my $num_worker_threads = shift;
  return read_all_relay_log( $self, $log, $num_worker_threads, 1, 1 );
}

# wait until slave executes all relay logs.
# MASTER_LOG_POS() must not be used
sub wait_until_relay_log_applied($$$) {
  my $self               = shift;
  my $log                = shift;
  my $num_worker_threads = shift;
  return read_all_relay_log( $self, $log, $num_worker_threads, 1 );
}

sub read_all_relay_log {
  my $self                 = shift;
  my $log                  = shift;
  my $num_worker_threads   = shift;
  my $wait_until_latest    = shift;
  my $io_thread_should_run = shift;
  $wait_until_latest    = 0 if ( !defined($wait_until_latest) );
  $io_thread_should_run = 0 if ( !defined($io_thread_should_run) );
  my $sql_thread_check;

  my %status;
  do {
    $sql_thread_check = 1;
    %status           = $self->check_slave_status();
    if ( $status{Status} != 0 ) {
      return %status;
    }
    elsif ( !$io_thread_should_run && $status{Slave_IO_Running} eq "Yes" ) {
      $status{Status} = 3;
      $status{Errstr} = "Slave IO thread is running! Check master status.";
      return %status;
    }
    elsif ( $status{Slave_SQL_Running} eq "No" ) {
      $status{Status} = 4;
      $status{Errstr} = "SQL thread is not running! Check slave status.";
      return %status;
    }
    elsif ( ( $status{Master_Log_File} eq $status{Relay_Master_Log_File} )
      && ( $status{Read_Master_Log_Pos} == $status{Exec_Master_Log_Pos} ) )
    {
      $status{Status} = 0;
      return %status;
    }

    if ($io_thread_should_run) {
      if (!$status{Slave_IO_State}
        || $status{Slave_IO_State} !~ m/Waiting for master to send event/ )
      {
        $sql_thread_check = 0;
      }
    }
    if ($sql_thread_check) {
      my $sql_thread_done    = 0;
      my $worker_thread_done = 0;
      my $current_workers    = 0;
      my $sth                = $self->{dbh}->prepare(Show_Processlist_SQL);
      $sth->execute();
      while ( my $ref = $sth->fetchrow_hashref ) {
        my $user  = $ref->{User};
        my $state = $ref->{State};
        if ( defined($user)
          && $user eq "system user"
          && defined($state) )
        {
          if ( $state =~ m/^Has read all relay log/
            || $state =~ m/^Slave has read all relay log/ )
          {
            $sql_thread_done = 1;
            if ( $num_worker_threads == 0 ) {
              $worker_thread_done = 1;
            }
            if ($worker_thread_done) {
              last;
            }
          }
          elsif ( $state =~ m/^Waiting for an event from Coordinator/ ) {
            $current_workers++;
            if ( $current_workers >= $num_worker_threads ) {
              $worker_thread_done = 1;
            }
          }
        }
        if ( $worker_thread_done == 1 && $sql_thread_done == 1 ) {
          last;
        }
      }
      if ( $sql_thread_done == 1 && $worker_thread_done == 1 ) {
        $status{Status} = 0;
        return %status;
      }
      $log->debug(
        sprintf(
          "Sql Thread Done: %d, Worker Thread done: %d, Ended workers: %d",
          $sql_thread_done, $worker_thread_done, $current_workers
        )
      );
    }
  } while ( $wait_until_latest && sleep(1) );

  $status{Status} = 1;
  $status{Errstr} =
    "Unexpected error happened on waiting reading all relay logs.";
  return %status;
}

sub get_threads_util {
  my $dbh                    = shift;
  my $my_connection_id       = shift;
  my $running_time_threshold = shift;
  my $type                   = shift;
  $running_time_threshold = 0 unless ($running_time_threshold);
  $type                   = 0 unless ($type);
  my @threads;

  my $sth = $dbh->prepare(Show_Processlist_SQL);
  $sth->execute();

  while ( my $ref = $sth->fetchrow_hashref() ) {
    my $id         = $ref->{Id};
    my $user       = $ref->{User};
    my $host       = $ref->{Host};
    my $command    = $ref->{Command};
    my $state      = $ref->{State};
    my $query_time = $ref->{Time};
    my $info       = $ref->{Info};
    $info =~ s/^\s*(.*?)\s*$/$1/ if defined($info);
    next if ( $my_connection_id == $id );
    next if ( defined($query_time) && $query_time < $running_time_threshold );
    next if ( defined($command)    && $command =~ /^Binlog Dump/ );
    next if ( defined($user)       && $user eq "system user" );
    next if ( defined($user)       && $user eq "event_scheduler" );

    if ( $type >= 1 ) {
      next if ( defined($command) && $command eq "Sleep" );
      next if ( defined($command) && $command eq "Connect" );
    }

    if ( $type >= 2 ) {
      next if ( defined($info) && $info =~ m/^select/i );
      next if ( defined($info) && $info =~ m/^show/i );
    }
    push @threads, $ref;
  }
  return @threads;
}

sub print_threads_util {
  my ( $threads_ref, $max_prints ) = @_;
  my @threads = @$threads_ref;
  my $count   = 0;
  print "Details:\n";
  foreach my $thread (@threads) {
    print Data::Dumper->new( [$thread] )->Indent(0)->Terse(1)->Dump . "\n";
    $count++;
    if ( $count >= $max_prints ) {
      printf( "And more.. (%d threads in total)\n", $#threads + 1 );
      last;
    }
  }
}

sub get_threads($$$) {
  my $self                   = shift;
  my $running_time_threshold = shift;
  my $type                   = shift;
  return MHA::DBHelper::get_threads_util( $self->{dbh}, $self->{connection_id},
    $running_time_threshold, $type );
}

sub get_running_threads($$) {
  my $self                   = shift;
  my $running_time_threshold = shift;
  return $self->get_threads( $running_time_threshold, 1 );
}

sub get_running_update_threads($$) {
  my $self                   = shift;
  my $running_time_threshold = shift;
  return $self->get_threads( $running_time_threshold, 2 );
}

sub kill_threads {
  my ( $self, @threads ) = @_;
  foreach (@threads) {
    kill_thread_util( $self->{dbh}, $_->{Id} );
  }
}

sub kill_thread_util {
  my $dbh = shift;
  my $id  = shift;
  eval {
    my $sth = $dbh->prepare("KILL ?");
    $sth->execute($id);
  };
  if ($@) {
    my $mysql_err = $dbh->err;
    if ( $mysql_err && $mysql_err == $MHA::ManagerConst::MYSQL_UNKNOWN_TID ) {
      $@ = undef;
      return;
    }
    croak $@;
  }
}

sub rename_user($$$) {
  my $self      = shift;
  my $from_user = shift;
  my $to_user   = shift;

  my $query = sprintf( Rename_User_SQL, $from_user, $to_user );
  return $self->execute($query);
}

sub execute_ddl($$) {
  my ( $self, $query ) = @_;
  return $self->execute($query);
}

sub master_pos_wait($$$) {
  my $self        = shift;
  my $binlog_file = shift;
  my $binlog_pos  = shift;
  my $sth         = $self->{dbh}->prepare(Master_Pos_Wait_NoTimeout_SQL);
  $sth->execute( $binlog_file, $binlog_pos );
  my $href = $sth->fetchrow_hashref;
  return $href->{Result};
}

sub gtid_wait($$) {
  my $self      = shift;
  my $exec_gtid = shift;
  my $sth       = $self->{dbh}->prepare(Gtid_Wait_NoTimeout_SQL);
  $sth->execute($exec_gtid);
  my $href = $sth->fetchrow_hashref;
  return $href->{Result};
}

sub _blocked_password {
  my $password = shift;
  if ( $password eq '' ) {
    return Blocked_Empty_Password;
  }
  elsif ( length($password) == Old_Password_Length ) {
    return Blocked_Old_Password_Head . $password;
  }
  elsif ( $password =~ Released_New_Password_Regexp ) {
    return join( "", reverse( split //, $password ) );
  }
  else {
    return;
  }
}

sub _released_password {
  my $password = shift;
  if ( $password eq Blocked_Empty_Password ) {
    return '';
  }
  elsif ( index( $password, Blocked_Old_Password_Head ) == 0 ) {
    return substr( $password, length(Blocked_Old_Password_Head) );
  }
  elsif ( $password =~ Blocked_New_Password_Regexp ) {
    return join( "", reverse( split //, $password ) );
  }
  else {
    return;
  }
}

sub _block_release_user_by_regexp {
  my ( $dbh, $user, $host, $block ) = @_;
  my $users_to_block =
    $dbh->selectall_arrayref( Select_User_Regexp_SQL, { Slice => {} },
    $user, $host );
  my $failure = 0;
  for my $u ( @{$users_to_block} ) {
    my $password =
      $block
      ? _blocked_password( $u->{password} )
      : _released_password( $u->{password} );
    if ( defined $password ) {
      my $ret =
        $dbh->do( Set_Password_SQL, undef, $u->{user}, $u->{host}, $password );
      unless ( $ret eq "0E0" ) {
        $failure++;
      }
    }
  }
  return $failure;
}

sub block_user_regexp {
  my ( $self, $user, $host ) = @_;
  return _block_release_user_by_regexp( $self->{dbh}, $user, $host, 1 );
}

sub release_user_regexp {
  my ( $self, $user, $host ) = @_;
  return _block_release_user_by_regexp( $self->{dbh}, $user, $host, 0 );
}

1;
