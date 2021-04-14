"""
AWS Lambda function for transferring files from S3 to SFTP on a create event.

Style note: the specific S3 interactions have been split out into very simple
one line functions - this is to make the code easier to read and test. It would
be perfectly valid to just have a single function that runs the entire thing.

Required env vars:

    SSH_HOSTNAME
    SSH_USERNAME
    SSH_PASSWORD or SSH_PRIVATE_KEY (S3 file path in 'bucket:key' format)

Optional env vars

    SSH_PORT - defaults to 22
    SSH_DIR - if specified the SFTP client will transfer the files to the
        specified directory.
    SSH_FILENAME - used as a mask for the remote filename. Supports three
        string replacement vars - {bucket}, {key}, {current_date}. Bucket
        and key refer to the uploaded S3 file. Current date is in ISO format.

"""

import json
import urllib.parse
import boto3
import io
import logging
import os
import botocore.exceptions
import paramiko

logger = logging.getLogger()
logger.setLevel(os.getenv('LOGGING_LEVEL', 'INFO'))

def on_trigger_event(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # read in shared properties on module load - will fail hard if any are missing
    FTP_HOST = os.environ['FTP_HOST']
    FTP_USERNAME = os.environ['FTP_USERNAME']
    # must have one of pwd / key - fail hard if both are missing
    FTP_PASSWORD = os.environ['FTP_PASSWORD']
    # path to a private key file on S3 in 'bucket:key' format.
    #FTP_PRIVATE_KEY = os.getenv('FTP_PRIVATE_KEY')
    #assert FTP_PASSWORD or FTP_PRIVATE_KEY, "Missing FTP_PASSWORD or FTP_PRIVATE_KEY"
    # optional
    FTP_PORT = int(os.getenv('FTP_PORT', 22))
    #SSH_DIR = os.getenv('SSH_DIR')
    # filename mask used for the remote file
    # SSH_FILENAME = os.getenv('SSH_FILENAME', 'data_{current_date}')
    logger.info(f"S3-SFTP: Establish connection to SFTP host '{FTP_HOST}'...")

    #if FTP_PRIVATE_KEY:
    #    key_obj = get_private_key(*FTP_PRIVATE_KEY.split(':'))
    #else:
    key_obj = None

    # prefix all logging statements - otherwise impossible to filter out in
    # Cloudwatch
    logger.info(f"S3-SFTP: received trigger event")

    #s3Client = boto3.client('s3')

    #if event and event['Records']:
    #    for record in event['Records']:
    #        sourceBucket = record['s3']['bucket']['name']
    #        sourceKey = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')


            # Get the object from the event and show its content type
            # bucket = event['Records'][0]['s3']['bucket']['name']
            # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        sftp_client, transport = connect_to_sftp(
            hostname=FTP_HOST,
            port=FTP_PORT,
            username=FTP_USERNAME,
            password=FTP_PASSWORD,
            pkey=key_obj
        )
        #if SSH_DIR:
        #    sftp_client.chdir(SSH_DIR)
        #    logger.debug(f"S3-SFTP: Switched into remote SFTP upload directory")

        with transport:
            for s3_file in s3_files(event):
                filename = sftp_filename(s3_file)
                filesize = s3_file.content_length #size in bytes
                bucket = s3_file.bucket_name
                contents = ''
                try:
                    logger.info(f"S3-SFTP: Transferring S3 file '{s3_file.key}' to Axway under '{filename}' with file size '{filesize}'.")
                    transfer_file(sftp_client, s3_file, filename)
                except Exception as ex:
                    logger.exception(f"S3-SFTP: Error transferring S3 file '{s3_file.key}'.")
                #except botocore.exceptions.BotoCoreError as ex:
                #    logger.exception(f"S3-SFTP: Error transferring S3 file '{s3_file.key}'.")
                    #contents = str(ex)
                    #filename = filename + '.x'

                #logger.info(f"S3-SFTP: Archiving S3 file '{s3_file.key}'.")
                #archive_file(bucket=bucket, filename=filename, contents=contents)
                #logger.info(f"S3-SFTP: Deleting S3 file '{s3_file.key}'.")
                #delete_file(s3_file)

                return "ok";

                #response = s3Client.get_object(Bucket=sourceBucket, Key=sourceKey)
                #print("CONTENT TYPE: " + response['ContentType'])
                #print(response)

                #Only /tmp is writable in AWS Lambda.
                #os.chdir("/tmp/")

                #Download the file to /tmp/ folder
                #fileName = os.path.basename(sourceKey)
                #downloadPath = '/tmp/'+ fileName
                #print(downloadPath)
                #s3Client.download_file(sourceBucket, sourceKey, downloadPath)

                #FTP_PATH = '/'

                #with FTP(ftpHost, ftpUser, ftpPwd) as ftp, open(fileName, 'rb') as file:
                    #ftp.storbinary('STOR ' + fileName, file)
                 #   ftp.storbinary(f'STOR {FTP_PATH}{file.name}', file)


                #print("connect...")
                #ftp = FTP_TLS(ftpHost)
                #print("...connect done.")
                #print("connect login...")
                #ftp.login(user = ftpUser, passwd = ftpPwd)
                #print("...connect login done.")
                #ftp.cwd("box_dest")
                #print("connect start open...")
                #with open(fileName, 'r') as file:
            #        print("start bin...")
            #        ftp.storbinary('STOR ' + fileName, file)

    #            print('File transmitted!!!')

                #We don't need the file in /tmp/ folder anymore
                #os.remove(fileName)

    #            return
            #except ftplib.all_errors as e:
            #    print( 'Ftp fail -> ', e )
            #    return False

    except Exception as e:
        logger.exception(f"S3-SFTP: Error transferring S3 file.")
        raise e
#    except:
#        logger.error(f"S3-SFTP: Unexpected error:", sys.exc_info()[0])
#        raise

def connect_to_sftp(hostname, port, username, password, pkey):
    """Connect to SFTP server and return client object."""
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password, pkey=pkey)
    client = paramiko.SFTPClient.from_transport(transport)

    #client = SSHClient()
    #client.set_missing_host_key_policy(AutoAddPolicy)
    #client.connect("example.com")

    logger.debug(f"S3-SFTP: Connected to remote SFTP server")
    return client, transport

def get_private_key(bucket, key):
    """
    Return an RSAKey object from a private key stored on S3.
    It will fail hard if the key cannot be read, or is invalid.
    """
    key_obj = boto3.resource('s3').Object(bucket, key)
    key_str = key_obj.get()['Body'].read().decode('utf-8')
    key = paramiko.RSAKey.from_private_key(io.StringIO(key_str))
    logger.debug(f"S3-SFTP: Retrieved private key from S3")
    return key


def s3_files(event):
    """
    Iterate through event and yield boto3.Object for each S3 file created.
    This function loops through all the records in the payload,
    checks that the event is a file creation, and if so, yields a
    boto3.Object that represents the file.
    NB Redshift will trigger an `ObjectCreated:CompleteMultipartUpload` event
    will UNLOADing the data; if you select to dump a manifest file as well,
    then this will trigger `ObjectCreated:Put`
    Args:
        event: dict, the payload received from the Lambda trigger.
            See tests.py::TEST_RECORD for a sample.
    """
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        event_category, event_subcat = record['eventName'].split(':')

        logger.info(f"S3-SFTP: Receive event of category '{ event_category }' and '{ event_subcat }'")

        if event_category == 'ObjectCreated':
            logger.info(f"S3-SFTP: Received '{ event_subcat }' trigger on '{ key }'")
            yield boto3.resource('s3').Object(bucket, key)
        else:
            logger.warning(f"S3-SFTP: Ignoring invalid event: { record }")


def sftp_filename(s3_file):
    """Create destination SFTP filename."""
    file_name = os.path.basename(s3_file.key)
    logger.info(f"S3-SFTP: S3 File bucket name '{ s3_file.bucket_name }' and key '{ s3_file.key }' and extracted file name '{ file_name }'")

    return file_name
    ##file_mask.format(
    ##    bucket=s3_file.bucket_name,
    ##    key=s3_file.key.replace("_000", ""),
    ##    current_date=datetime.date.today().isoformat()
    ##)


def transfer_file(sftp_client, s3_file, filename):
    """
    Transfer S3 file to SFTP server.
    Args:
        sftp_client: paramiko.SFTPClient, connected to SFTP endpoint
        s3_file: boto3.Object representing the S3 file
        filename: string, the remote filename to use
    Returns a 2-tuple containing the name of the remote file as transferred,
        and any status message to be written to the archive file.
    """

    #ssh = paramiko.SSHClient()
    #ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #ssh.connect(host, port, username, password, key)

    # Using the SSH client, create a SFTP client.
    #sftp = ssh.open_sftp()
    #sftp.listdir(path='.')
    sftp_client.listdir(path='.')

    with sftp_client.file(filename, 'w') as sftp_file:
        s3_file.download_fileobj(Fileobj=sftp_file)

    logger.info(f"S3-SFTP: Transferred S3 file '{ s3_file.key }' from S3 to SFTP as '{ filename }'")

    sftp_client.listdir(path='.')


def delete_file(s3_file):
    """
    Delete file from S3.
    This is only a one-liner, but it's pulled out into its own function
    to make it easier to mock in tests, and to make the trigger
    function easier to read.
    Args:
        s3_file: boto3.Object representing the S3 file
    """
    try:
        s3_file.delete()
    except botocore.exceptions.BotoCoreError as ex:
        logger.exception(f"S3-SFTP: Error deleting '{ s3_file.key }' from S3.")
    else:
        logger.info(f"S3-SFTP: Deleted '{ s3_file.key }' from S3")


def archive_file(*, bucket, filename, contents):
    """
    Write to S3 an archive file.
    The archive does **not** contain the file that was sent, as we don't
    want the data hanging around on S3. Instead it's just an empty marker
    that represents the file. If the transfer errored, then the archive file
    has a '.x' suffix, and will contain the error message.
    Args:
        bucket: string, S3 bucket name
        filename: string, the name of the archive file
        contents: string, the contents of the archive file - blank unless there
            was an exception, in which case the exception message.
    """
    key = 'archive/{}'.format(filename)
    try:
        boto3.resource('s3').Object(bucket, key).put(Body=contents)
    except botocore.exceptions.BotoCoreError as ex:
        logger.exception(f"S3-SFTP: Error archiving '{ filename }' as '{ key }'.")
    else:
        logger.info(f"S3-SFTP: Archived '{ filename }' as '{ key }'.")
