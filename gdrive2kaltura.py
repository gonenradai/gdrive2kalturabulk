"""
Google Drive Media Extractor for Kaltura Upload:

    This script processes a specified Google Drive folder to identify media files
    (audio, video, and images) and produces a CSV file suitable for bulk upload
    to the Kaltura platform. It traverses through all subfolders,
    captures metadata about the media files, and appends them to the CSV. Media
    files are determined based on their MIME type.

    The script is multithreaded. Scan folder threads process a list of folders,
    while download threads download the files to a temporary storage.
    The files are then uploaded to s3 and an s3 download url is provided for
    the bulk upload csv.
    In case of HEIC image files which are not supported by Kaltura, the file
    is first converted to png and a .png extension is added to the file name
    In order to avoid duplicate entries the gdrive file id is set as the entry
    reference id. The script looks for a ready entry with the reference id
    and skips it. A local cache file is maintained in order to reduce calls.

    Author Metadata:
    - Name: Zohar Babin
    - Date: October 11, 2023
    - Contact: @zoharbabin on GitHub / @zohar on X.com
    - License: MIT

    Setup:
        1. Obtain a service account key from the Google Cloud Console:
            a. Go to the Google Cloud Console (https://console.cloud.google.com/).
            b. Navigate to IAM & Admin > Service accounts.
            c. Create a new service account or select an existing one.
            d. Under "Keys", add a new JSON key.
            e. Save the downloaded JSON file as 'credentials.json' in the script's directory.
            f. Ensure the service account has permissions for Drive API access and can
                read metadata from the desired Google Drive folder.

        2. Install required packages via pip:
            - pandas
            - google-auth, google-auth-oauthlib, google-auth-httplib2
            - google-api-python-client
            - pyheif
            - pillow
            - boto3
            - KalturaApiClient

    Usage:
        Run the script from the command line and provide the Google Drive folder ID
        and optionally a root category name for the Kaltura CSV:
        python script_name.py {folder_id} {drive_name} {root_category_name} {partnerId} {admin secret} {metadata_profile_id}

    Note: Ensure 'credentials.json' is present in the working directory and contains
        the necessary permissions.
"""

from __future__ import print_function


from datetime import datetime
import os.path
import sys
import io
from threading import Thread, Lock
import threading
from time import sleep
import argparse
from pathlib import Path
import pandas as pd
from PIL import Image
import pyheif

# Google client
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# AWS client
import boto3
from botocore.errorfactory import ClientError

# Kaltura client
from KalturaClient import *
from KalturaClient.Plugins.Core import KalturaSessionType
from KalturaClient.Plugins.Core import KalturaBaseEntryFilter
from KalturaClient.Plugins.Core import KalturaFilterPager

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive.readonly']

lock = Lock()
folders = []
scanned = []
running_scan_threads = []
running_dl_threads = []
max_running_scan = 0
cache = []

files = []
xprint = print
lock2 = Lock()

def print(*args, **kwargs):
    global lock2
    with lock2:
        try:
            now = datetime.now()
            date_time = now.strftime("%Y-%m-%d-%H:%M:%S.%f")
            output = io.StringIO()
            xprint(*args, file=output, **kwargs)
            contents = output.getvalue()
            output.close()
            xprint(date_time, threading.get_ident(), contents, end = '')
        except Exception as e:
            xprint('ZZZZZZZ',e)
            pass

def convert_heic_2_png(fname):
    try:
        print(f"convert_heic_2_png {fname}")
        heif_file = pyheif.read(fname)
        image = Image.frombytes(
         heif_file.mode,
         heif_file.size,
         heif_file.data,
         "raw",
        heif_file.mode,
        heif_file.stride,
    )

        image.save(f"{fname}.png", "PNG")
        return True
    except Exception as e:
        # Return False if something went wrong
        print("convert_heic_2_png", e)

    return False


def safe_pop(lock, listobj):
    with lock:
        value = listobj.pop(0)

    return value


def safe_append(lock, listobj, value):
    with lock:
        listobj.append(value)


def safe_len(lock, listobj):
    with lock:
        listLen = len(listobj)

    return listLen

# Function to append a value to a file
def safe_append_value_to_cache(value):
    with lock:
        value = str(value)
        with open(imported_file_name, 'a') as file:
            file.write(value + '\n')
        cache.add(value)

# Function to check if a value exists in a file
def safe_check_value_in_cache(value):
    with lock:
        return str(value) in cache

def get_s3_object_size(client, bucket_name, object_name):
    size = False

    try:
        print('get s3_object_size', bucket_name, object_name)
        response = client.head_object(Bucket=bucket_name, Key=object_name)
        size = response['ContentLength']
    except ClientError as e:
        #print(e)
        pass

    return size


def upload_to_s3(client, bucket_name, done_path, object_name):
    try:
        print('upload_to_s3', bucket_name, done_path, object_name)
        response = client.upload_file(done_path, bucket_name, object_name)
        os.remove(done_path)
        return True

    #except ClientError as e:
    except Exception as e:
        print("failed to upload to s3", e)

    return False



def download_thread(creds):
    service = build('drive', 'v3', credentials=creds)
    ident = threading.get_ident()

    while True:
        with lock:
            item = files.pop(0) if len(files) else False
            l = len(running_scan_threads)

        if not item:
            with lock:
                if ident in running_dl_threads:
                    running_dl_threads.remove(ident)

            if not len(running_scan_threads):
                break

            sleep(0.1)
            continue

        with lock:
            if not ident in running_dl_threads:
                running_dl_threads.append(ident)
            #print("runningnDLThreads", len(running_dl_threads))

        gitem = item['gitem']
        file_id = gitem['id']
        if check_file_imported(file_id):
            continue

        file_name = gitem['name']
        folder_id = item['folder_id']

        file_size = int(gitem['size']) if 'size' in gitem else False
        if not file_size:
            continue

        # if object isn't on s3, download from drive and upload to s3
        object_name = f"{folder_id}/{file_id}"
        if args.folder_id != folder_id:
            object_name = f"{args.folder_id}/{object_name}"

        client = boto3.client("s3")

        # heic images are not supported by Kaltura and pre converted to png by the script, check for them on s3
        exists_on_s3 = False
        if object_name.lower().endswith('.heic') and get_s3_object_size(client, bucket_name, object_name + '.png'):
            exists_on_s3 = True
            object_name += '.png'
        else:
            size = get_s3_object_size(client, bucket_name, object_name)
            exists_on_s3 = size == file_size

        if not exists_on_s3:
            done_path = download_file(service, file_id, file_name)

            if not done_path:
                print(f"Failed to download {folder_id} {file_id} {file_name}")
                continue

            if done_path.lower().endswith('.heic'):
                if convert_heic_2_png(done_path):
                    os.remove(done_path)
                    done_path += '.png'
                    object_name += '.png'

            if not upload_to_s3(client, bucket_name, done_path, object_name):
                print(f"Failed to upload to s3 {folder_id} {file_id} {file_name} {object_name}")
                continue

        folder_name = item['folder_name']
        folder_path = item['folder_path']
        mime_type = gitem['mimeType']
        media_type = mime_type.split('/')[0]  # Extracts "audio", "video", or "image" from mime_type
        user_name = gitem['owners'][0]['displayName']
        user_email = gitem['owners'][0]['emailAddress']
        file_extension = gitem.get('fileExtension', '')
        description = gitem.get('description', '')
        #download_url = f'https://drive.google.com/uc?export=download&id={file_id}'
        download_url = f'https://stand4israel-content-bucket.s3.amazonaws.com/{object_name}'
        row_dict = {
            '*title': file_name,
            'description': f'By {user_name} in {folder_name}. \n{description}',
            'tags': '',
            'url': download_url,
            'referenceId': file_id,
            'contentType': media_type.capitalize(),
            'category': f'{category_name}>{drive_name} {folder_path}',
            'metadataProfileId': metadata_profile_id,
            'creatorId': user_email,
            'ownerId': 'Admins'
        }

        rows_list.append(row_dict)




def build_file_name(file_id, name):
    return f'{file_id}-{name}'


def download_file(service, file_id, name):
    global dl_folder, temp_folder

    file_name = build_file_name(file_id, name)
    final_path = f'{dl_folder}/{file_name}'

    if final_path.lower().endswith('.heic') and os.path.exists(final_path + '.png'):
        final_path += '.png'
        print(f"File exists {final_path}")
        return final_path

    if (os.path.exists(final_path)):
        print(f"File exists {final_path}")
        return final_path

    request = service.files().get_media(fileId=file_id)

    temp_path = f'{temp_folder}/{file_name}'

    fh = io.FileIO(temp_path, mode = 'wb')

    # Initialise a downloader object to download the file
    downloader = MediaIoBaseDownload(fh, request, chunksize=5*1000000)
    done = False

    try:
        # Download the data in chunks
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Download {file_name} %d%%" % int(status.progress() * 100))

        os.rename(temp_path, final_path)
        print(f"File Downloaded {file_name}")

        return final_path

    except Exception as e:
        # Return False if something went wrong
        print(f"download_file failed {file} {name}.", e)

    return False


def list_folder(creds):
    service = build('drive', 'v3', credentials=creds)

    next_page_token = ''
    page = 0
    ident = threading.get_ident()

    while True:
        if not len(next_page_token):
            if safe_len(lock, folders):
                with lock:
                    if not ident in running_scan_threads:
                        running_scan_threads.append(ident)

                folder_item = safe_pop(lock, folders)
                folder_id = folder_item['id']
                folder_name = folder_item['name']
                folder_path = folder_item['path']

                query = f"'{folder_id}' in parents"
                if query in scanned:
                    continue

                page = 0

                print("FOLDER ", folder_id)
                safe_append(lock, scanned, query)
            else:
                with lock:
                    if ident in running_scan_threads:
                        running_scan_threads.remove(ident)

                sleep(0.1)
                if not safe_len(lock, running_scan_threads):
                    break

                continue

        page = page + 1
        print("LIST", folder_id, next_page_token, page)
        with lock:
            l = len(running_scan_threads)
            global max_running_scan
            if l > max_running_scan:
                max_running_scan = l

        try:
            # Call the Drive v3 API
            results = service.files().list(q=query,
                fields="nextPageToken, files(id, name, mimeType, owners, description, size, kind, fileExtension, fileExtension, capabilities/canListChildren, capabilities/canDownload, shortcutDetails)",
                pageToken = next_page_token,
                pageSize = 1000).execute()

            items = results.get('files', [])
            next_page_token = results.get('nextPageToken', '')

            if not items:
                print('No files found.', folder_id)
                continue

            for item in items:
                if item['capabilities']['canDownload'] and 'size' in item and int(item['size']) > 0 and item['mimeType'].startswith(('image/', 'video/', 'audio/')):
                    print("PROCESS ",item['size'],item['id'],item['name'])
                    safe_append(lock, files, {'gitem': item, 'folder_id': folder_id, 'folder_name': folder_name, 'folder_path': folder_path}) #'id': item['id'], 'name' : item['name']})

            new_folder_path = f'{folder_path} - {item["name"]}'

            for item in items:
                new_id = False
                if item['capabilities']['canListChildren']:
                    new_id = item['id']
                elif 'shortcutDetails' in item:
                    new_id = item['shortcutDetails']['targetId']

                if new_id:
                    safe_append(lock, folders, {'id': new_id, 'name': folder_name, 'path': new_folder_path})

                print(u'FILE {2} {1} {0}'.format(item['name'], item['id'], item['size'] if 'size' in item else 0))
                #print(item)
        except HttpError as error:
            # TODO(developer) - Handle errors from drive API.
            print(f'An error occurred: {error}')

    print("DONE list_folder thread")

def check_file_imported(file_id):
    if safe_check_value_in_cache(file_id):
        return True

    """
    Makes API call to Kaltura to search for a READY entry with the given file_id as referenceId
    """
    print("check_file_imported", file_id)
    config = KalturaConfiguration()
    config.serviceUrl = "https://www.kaltura.com/"
    ks = KalturaClient.generateSessionV2(admin_secret, "test",
        KalturaSessionType.ADMIN, partner_id,
        86400, "disableentitlement")
    client = KalturaClient(config)
    client.setKs(ks)

    filter = KalturaBaseEntryFilter()
    filter.setReferenceIdEqual(file_id)

    pager = KalturaFilterPager()
    pager.setPageSize(1)

    # Perform the search using the list action
    results = client.baseEntry.list(filter, pager)

    # Check if there are any results
    ret = (results.getTotalCount() > 0)
    if ret:
        safe_append_value_to_cache(file_id)

    return ret


def upload_bulk(csv_file_name):
    #sys.exit(0)
    config = KalturaConfiguration()
    config.serviceUrl = "https://www.kaltura.com/"
    ks = KalturaClient.generateSessionV2(admin_secret, "test",
        KalturaSessionType.ADMIN, partner_id,
        86400, "disableentitlement")
    client = KalturaClient(config)
    client.setKs(ks)

    result = client.media.bulkUploadAdd(open(csv_file_name, 'rb'))



def main():
    return

if __name__ == '__main__':
    # Argument parsing to get folder_id and root_category_name from command line
    parser = argparse.ArgumentParser(description='Process a Google Drive folder to create a Kaltura bulk upload CSV.')
    parser.add_argument('folder_id', help='The ID of the Google Drive folder to process.')
    parser.add_argument('drive_name', help='The name of the Google Drive folder to process to be set as mediaSource in metadata')
    parser.add_argument('partner_id', help='partner ID to check entry exists by reference ID')
    parser.add_argument('admin_secret', help='admin secret for the partner ID')
    parser.add_argument('root_category_name', nargs='?', default='', help='If provided, will be appended to all categories as the root in the Kaltura CSV')
    parser.add_argument('metadata_profile_id', nargs='?', default='', help='If provided, will be set as the metadata profile ID')

    args = parser.parse_args()

    folders.append({'id': args.folder_id, 'name': 'Root', 'path': ''})

    bucket_name = 'stand4israel-content-bucket'

    # Prepare download folders
    temp_folder = f"done-tmp/{args.folder_id}"
    Path(temp_folder).mkdir(parents = True, exist_ok = True)

    dl_folder = f"done/{args.folder_id}"
    Path(dl_folder).mkdir(parents = True, exist_ok = True)

    # Global list to store rows for CSV
    rows_list = []

    # verifying a cache file exists that would hold list of drive files IDs that have been imported already to the given PID
    imported_file_name = f'cache-{args.folder_id}'
    Path(imported_file_name).touch()

    with open(imported_file_name, 'r') as file:
        lines = file.readlines()
    cache = set([s.strip('\n') for s in lines])


    partner_id = args.partner_id
    admin_secret = args.admin_secret
    category_name = args.root_category_name
    drive_name = args.drive_name
    metadata_profile_id = args.metadata_profile_id

    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    """

    # Load client secrets for Google Drive API
    creds = service_account.Credentials.from_service_account_file(
        './credentials.json', scopes=['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive.readonly'])

    try:
        threads = []
        for i in range(10):
            t = Thread(target=list_folder, args=(creds, ))
            threads.append(t)
            t.start()

        dlThreads = []
        for i in range(10):
            t = Thread(target=download_thread, args=(creds, ))
            dlThreads.append(t)
            t.start()

        for i in threads:
            i.join()

        for i in dlThreads:
            i.join()

        print("max_running_scan", max_running_scan)

        df = pd.DataFrame(rows_list)
        csv_file_name = f'kaltura_upload-{args.folder_id}.csv'
        df.to_csv(csv_file_name, index=False, encoding='utf-8-sig')
        if(len(rows_list) > 0):
            upload_bulk(csv_file_name)
        else:
            print("Nothing to bulk upload")


    except Exception as e:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {e}')
