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

CONFIG_FILE_NAME = "watch.cfg"

class WatcherException(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return repr(self.message)

def shutdown_handler(signum, frame):
    sys.exit(1)

def process_zip(zip):
    pass 

def move_zip(file, directory):
    destination = os.path.join(directory,os.path.basename(file))
    os.rename(file, destination)

def validate_metadata(metadata_handle, zipfile, zip_name):
    # this is specific to the client
    metadata = csv.reader(metadata_handle)
    if(csv_title_row):
        metadata.next()
    objects = []
    for row in metadata:
        object = {}
        # grab a row and validate it appropriatly
        object['files'] = row[0].split(';')
        object['line_num'] = metadata.line_num
        for index, file in enumerate(object['files']):
            file = string.strip(file)
            object['files'][index] = file
            if file not in zipfile.namelist():
                raise WatcherException('Metadata validation failure. File %s not found in zipfile %s. metadata.csv:%d' % (file, zip_name, metadata.line_num))
        object['title'] = row[1]
        object['relation'] = row[2].split(' ')
        if len(object['relation']) != 3:
            raise WatcherException('Metadata validation failure. Relation: %s is not valid. metadata.csv:%d' % (row[2], object['line_num']))
        if object['relation'][0] not in object['files']:
            logger.debug(object['relation'][0])
            logger.debug(object['files'])
            raise WatcherException('Metadata validation failure. Relation: %s is not valid. metadata.csv:%d' % (row[2], object['line_num']))
        if object['relation'][2] not in object['files']:
            logger.debug(object['relation'][2])
            logger.debug(object['files'])
            raise WatcherException('Metadata validation failure. Relation: %s is not valid. metadata.csv:%d' % (row[2], object['line_num']))
        object['subjects'] = row[3].split(';')
        object['keywords'] = row[4].split(';')
        object['date'] = row[5]
        object['spacial'] = row[6]
        object['temporal'] = row[7]
        roles = row[8].split(';')
        first_names = row[9].split(';')
        last_names = row[10].split(';')
        if not (len(roles) == len(first_names) == len(last_names)):
            raise WatcherException('Metadata validation failure. Length of Roles(%d), FirstNames(%d) and LastNames(%d)' 
                'is not consistant. metadata.csv:%d' % (len(roles), len(first_names), len(last_names), object['line_num']))
        object['people'] = []
        for role, first, last in zip(roles, first_names, last_names):
            person = {}
            person['first'] = first
            person['last'] = last
            person['role'] = role
            object['people'].append(person)
        object['publisher'] = row[11]
        object['language'] = row[12]
        object['rights'] = row[13]
        object['abstract'] = row[14]
        object['significat'] = row[15]
        object['sensitive'] = row[16]
        object['notes'] = row[17]
        logger.debug(object)
        objects.append(object)
    return objects

def create_objects(objects, zip, client):

    pretty = pprint.PrettyPrinter(indent=4)
    for object in objects:

        pid = client.getNextPID(unicode(repository_namespace))
        logger.debug(pid)
        logger.debug(pretty.pformat(object))
        obj = client.createObject(pid, label=unicode(object['title']))

        for file in object['files']:
            #file_handle = zip.open(file)
            mime,encoding = mimetypes.guess_type(file)
            logger.debug(mime)
            obj.addDataStream(file, zip.read(file), label=unicode(file), mimeType=unicode(mime), controlGroup=u'M')
        
        obj.addDataStream('METADATA', pretty.pformat(object), mimeType=u'text/plain', controlGroup=u'M')
        logger.debug('finished processing')


if __name__ == '__main__':
    # register handlers so we properly disconnect and reconnect
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # parse the passed in command line options
    # and read the passed in config file if it exists
    configp = ConfigParser.SafeConfigParser()
    optionp = OptionParser()

    optionp.add_option('-C', '--config-file', type = 'string', dest = 'configfile', default = CONFIG_FILE_NAME,
                  help = 'Path of the configuration file.')

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


