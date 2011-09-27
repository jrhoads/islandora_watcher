import ConfigParser
import logging
import logging.handlers
import os
import errno
import sys
import time
import csv
import signal
import fcrepo.connection
import glob
import zipfile
import mimetypes
import pprint
import string
from fcrepo.client import FedoraClient
from fcrepo.connection import Connection as FedoraConnection
from fcrepo.connection import FedoraConnectionException
from optparse import OptionParser

DEFAULT_CONFIG_FILE = "watch.cfg"
DEFAULT_WATCH_DIRECTORY = "watch"
DEFAULT_POLL_TIME = 30

def shutdown_handler(signum, frame):
    sys.exit(1)

if __name__ == '__main__':

    # register handlers so we properly disconnect and reconnect
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # parse the passed in command line options
    # and read the passed in config file if it exists
    configp = ConfigParser.SafeConfigParser()
    optionp = OptionParser()

    optionp.add_option('-c', '--config-file', type = 'string', dest = 'configfile', default = DEFAULT_CONFIG_FILE,
                  help = 'Path of the configuration file.')

    optionp.add_option('-r', '--run-once', action = 'store_false', dest = 'watch', default = True,
                  help = 'Run once and exit. Suitable for adding to a cronjob.')

    optionp.add_option('-d', '--directory', type = 'string', dest = 'directory', default = DEFAULT_WATCH_DIRECTORY,
                  help = 'Directory to run the batch ingestor on.')

    optionp.add_option('-p', '--poll-time', type = 'int', dest = 'poll_time', default = DEFAULT_POLL_TIME,
                  help = 'The amount of time in seconds between directory polling.')

    (options, args) = optionp.parse_args()

    if not os.path.exists(options.configfile):
        print 'Config file %s not found!' % (options.configfile)
        optionp.print_help()
        sys.exit(1)

    # load config file values and give error if its incorrect
    try:
        configp.read(options.configfile)

        #csv
        csv_title_row = configp.getboolean('CSV', 'title_row')

        #watcher
        watcher_poll = configp.getint('DirectoryWatcher', 'poll_time')
        watcher_dir = configp.get('DirectoryWatcher', 'directory')

        #repository server
        repository_user = configp.get('Fedora', 'username')
        repository_pass = configp.get('Fedora', 'password')
        repository_url = configp.get('Fedora', 'url')
        repository_namespace = configp.get('Fedora', 'namespace')

        #logging
        log_filename = configp.get('Logging', 'file')
        log_level = configp.get('Logging', 'level')
        log_max_size = configp.get('Logging', 'max_size')
        log_backup = configp.get('Logging', 'backup')

    except (ConfigParser.Error, ValueError), e:
        print 'Error reading config file %s' % options.configfile
        print e
        sys.exit(1)

    # setup logging
    levels = {'DEBUG':logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING,
      'ERROR':logging.ERROR, 'CRITICAL':logging.CRITICAL, 'FATAL':logging.FATAL}
    logging_format = '%(asctime)s %(levelname)s %(name)s %(message)s'
    date_format = '[%b/%d/%Y:%H:%M:%S]'
    log_handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=log_max_size, backupCount=log_backup)
    log_formatter = logging.Formatter(logging_format, date_format)
    log_handler.setFormatter(log_formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(log_handler)
    root_logger.setLevel(levels[log_level])
    logger = logging.getLogger('IslandoraDirectoryWatcher')

    # sanity check watcher dir
    if not os.path.isdir(watcher_dir):
        error = 'Directory: "%(dir)s" does not exist or is not a directory.' % {'dir':watcher_dir}
        logger.error(error)
        print error
        sys.exit(1)

    # setup a fedora connection
    try:
        fc = FedoraConnection(repository_url, username = repository_user, password = repository_pass)
        client = FedoraClient(fc)
    except Exception:
        logger.debug('Error connecting. URL: %s Username: %s Password: %s.' % (repository_url, repository_user, repository_pass));
        logger.error('Error connectiong to Fedora');
        sys.exit(1)

    # create the directory for bad files if it doesn't exist
    bad_file_directory = os.path.join(watcher_dir, 'BAD')
    try:
        os.makedirs(bad_file_directory)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise

    # create the directory for bad files if it doesn't exist
    complete_file_directory = os.path.join(watcher_dir, 'complete')
    try:
        os.makedirs(complete_file_directory)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise

    # watch a directory
    while 1:
        time.sleep (watcher_poll)
        zip_files = glob.glob (watcher_dir + "/*.zip")
        logger.debug(zip_files)
        for zip_file in zip_files:
            try:
                zip_handle = zipfile.ZipFile(zip_file, mode='r', allowZip64=True)
                logger.debug(zip_handle.namelist())
                try: 
                    if 'metadata.csv' in zip_handle.namelist():
                        metadata = zip_handle.open('metadata.csv')
                        objects = validate_metadata(metadata, zip_handle, os.path.basename(zip_file))
                        create_objects(objects, zip_handle, client)
                        move_zip(zip_file, complete_file_directory)
                        logger.info('Completed processing %s' % zip_file)
                    else:
                        raise WatcherException("Zipfile %s doesn't contain metadata.csv" % os.path.basename(zip_file))
                except WatcherException, e:
                    zip_handle.close()
                    logger.error(e.message)
                    move_zip(zip_file, bad_file_directory)

                    
            except (zipfile.BadZipfile, IOError), e:
                #this will happen while file is being uploaded.
                logger.debug('Exception opening ZIP file. %s' % e)


