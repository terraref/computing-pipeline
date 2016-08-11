# Monitor configuration options 
- _log_path_ - where to write log files
- _completed_tasks_path_ - where to write completed JSON metafiles

- _local_
  -- _incoming_files_path_ - local disk location of sensor data accessible by monitor; can be internal Docker directory that differs from FTP log lines
  -- _deletion_queue_ - local disk location where symlinks of processed files will be written; can be Docker internal
  -- _ftp_log_path_ - where to look for xferlog files; ignored if empty string
  -- _file_age_monitor_paths_ - list of folders to monitor for files older than X minutes to transfer
  -- _min_file_age_for_transfer_mins_ - minutes old a file must be to queue for transfer in file_age_monitor_paths folders
  -- _file_check_frequency_secs_ - how often to check FTP logs, monitored folders, etc. for new files to queue
  -- _max_pending_files_ - maximum # of files that can be queued for new Globus transfers
  -- _globus_transfer_frequency_secs_ - how often to generate new batches of Globus transfers

- _globus_
  -- _source_path_ - disk location of sensor data accessible by Globus; can be different to local disk location above
  -- _delete_path_ - disk location to write in symlinks; can differ from internal Docker directory that is invalid outside Docker
  -- _destination_path_ - Globus destination root directory; files will go here in same subdirs that follow source_path
  -- _source_endpoint_id_ - Globus source endpoint ID
  -- _destination_endpoint_id_ - Globus destination endpoint ID
  -- _username_ - Globus username
  -- _password_ - Globus password
  -- _authentication_refresh_frequency_secs_ - how often to attempt Globus reactivation
  -- _max_transfer_file_count_ - maximum # of files that can be sent in one Globus transfer
  -- _max_active_tasks_ - maximum # of Globus transfers that can be started without completion notification from NCSA

- _ncsa_api_
  -- _host_ - URL of NCSA monitor on ROGER that will 'catch' the Globus transfers
  -- _api_check_frequency_secs_ - how often to check with NCSA monitor for Globus transfer statuses

- _api_
  -- _port_ - port for this local API to listen on, i.e. to submit files to queue manually
  -- _ip_address_ - IP for the local API; one could POST to e.g. "http://0.0.0.0:5455/files"
