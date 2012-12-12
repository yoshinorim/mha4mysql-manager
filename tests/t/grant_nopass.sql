grant all privileges on *.* to 'admin' identified by '';
update mysql.user set password='' where user='admin';
flush privileges;
