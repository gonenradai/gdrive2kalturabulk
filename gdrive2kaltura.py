"""
Google Drive Media Extractor for Kaltura Upload:

This script processes a specified Google Drive folder to identify media files
(audio, video, and images) and produces a CSV file suitable for bulk upload 
to the Kaltura platform. It recursively traverses through all subfolders,
captures metadata about the media files, and appends them to the CSV. Media
files are determined based on their MIME type.

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
    - halo
    - KalturaApiClient

Usage:
Run the script from the command line and provide the Google Drive folder ID 
and optionally a root category name for the Kaltura CSV:
    python script_name.py {folder_id} {drive_name} {root_category_name} {partnerId} {admin secret} {metadata_profile_id} 

Note: Ensure 'credentials.json' is present in the working directory and contains 
the necessary permissions.
"""
import re
import argparse
import pandas as pd
import os
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from halo import Halo
from KalturaClient import *
from KalturaClient.Plugins.Core import KalturaSessionType
from KalturaClient.Plugins.Core import KalturaBaseEntryFilter
from KalturaClient.Plugins.Core import KalturaFilterPager

global imported_file_name
global partner_id
global admin_secret
global metadata_profile_id
global category_name
global drive_name

# Function to append a value to a file
def append_value_to_file(value):
    with open(imported_file_name, 'a') as file:
        file.write(str(value) + '\n')

# Function to check if a value exists in a file
def check_value_in_file(value):
    with open(imported_file_name, 'r') as file:
        for line in file:
            if line.strip() == str(value):
                return True
    return False


def check_file_imported(fileId):
    if check_value_in_file(fileId):
        return True
    """
    Makes API call to Kaltura to search for a READY entry with the given fileId as referenceId
    """
    print("making api call because not found in file")
    config = KalturaConfiguration()
    config.serviceUrl = "https://www.kaltura.com/"
    ks = KalturaClient.generateSessionV2(admin_secret, "test", 
                                             KalturaSessionType.ADMIN, partner_id, 
                                             86400, "disableentitlement")
    client = KalturaClient(config)
    client.setKs(ks)

    filter = KalturaBaseEntryFilter()
    filter.setReferenceIdEqual(fileId)

    pager = KalturaFilterPager()
    pager.setPageSize(1)

    # Perform the search using the list action
    results = client.baseEntry.list(filter, pager)

    # Check if there are any results
    ret = (results.getTotalCount() > 0)
    if ret:
        append_value_to_file(fileId)

    return ret


# Function to recursively list files and folders from a Google Drive folder and filter out media files.
def list_files(folder_id, folder_name, spinner, folder_path=''):
    """
    Recursively list files and folders from a Google Drive folder.

    Parameters:
    - folder_id (str): The ID of the Google Drive folder.
    - folder_name (str): Name of the current folder.
    - spinner (halo.Halo): Spinner instance for console output.
    - folder_path (str): Path of the current folder, default is empty.

    Returns:
    None. The function directly appends to the global rows_list.
    """

    # Query to fetch files and folders inside the current folder
    query = f"'{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name, mimeType, owners, description, kind, fileExtension, fileExtension)", pageSize=1000).execute()
    items = results.get('files', [])
    for item in items:
        mime_type = item['mimeType']
        file_id = item['id']
        file_name = item['name']

        # If the item is a folder, recursively call the function
        if mime_type == 'application/vnd.google-apps.folder':
            separator = '>' if folder_path else ''  # Avoid adding the separator before the first folder
            new_folder_path = f'{folder_path} - {file_name}'
            list_files(file_id, file_name, spinner, new_folder_path)

        # Check if the item is an image, video, or audio file
        elif mime_type.startswith(('image/', 'video/', 'audio/')):
            spinner.text = f'Processing {file_name} ({mime_type}) in folder {folder_path or "/"}'
            if(check_file_imported(file_id)):
                # print(f'skipping {file_id} because exists')
                continue
            media_type = mime_type.split('/')[0]  # Extracts "audio", "video", or "image" from mime_type
            user_name = item['owners'][0]['displayName']
            user_email = item['owners'][0]['emailAddress']
            file_extension = item.get('fileExtension', '')
            description = item.get('description', '')
            download_url = f'https://drive.google.com/uc?export=download&id={file_id}'
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

def upload_bulk():
    config = KalturaConfiguration()
    config.serviceUrl = "https://www.kaltura.com/"
    ks = KalturaClient.generateSessionV2(admin_secret, "test", 
                                             KalturaSessionType.ADMIN, partner_id, 
                                             86400, "disableentitlement")
    client = KalturaClient(config)
    client.setKs(ks)

    fileData = "./kaltura_upload.csv"
    result = client.media.bulkUploadAdd(open(fileData, 'rb'))

def main(folder_id):
    """
    Main function to process files from a Google Drive folder and save them in a CSV file.

    Parameters:
    - folder_id (str): The ID of the Google Drive folder.
    - root_category_name (str): Root category name to be appended to all categories in the CSV.

    Returns:
    None. Writes results to 'kaltura_upload.csv'.
    """
    with Halo(text='Processing files', spinner='dots') as spinner:
        list_files(folder_id, 'Root', spinner)
        spinner.succeed('Processing completed')
        df = pd.DataFrame(rows_list)
        df.to_csv('kaltura_upload.csv', index=False, encoding='utf-8-sig')
        if(len(rows_list) > 0):
            upload_bulk()
        else:
            print("Nothing to bulk upload")

if __name__ == "__main__":
    # Argument parsing to get folder_id and root_category_name from command line
    parser = argparse.ArgumentParser(description='Process a Google Drive folder to create a Kaltura bulk upload CSV.')
    parser.add_argument('folder_id', help='The ID of the Google Drive folder to process.')
    parser.add_argument('drive_name', help='The name of the Google Drive folder to process to be set as mediaSource in metadata')
    parser.add_argument('partner_id', help='partner ID to check entry exists by reference ID')
    parser.add_argument('admin_secret', help='admin secret for the partner ID')
    parser.add_argument('root_category_name', nargs='?', default='', help='If provided, will be appended to all categories as the root in the Kaltura CSV')
    parser.add_argument('metadata_profile_id', nargs='?', default='', help='If provided, will be set as the metadata profile ID')
    
    args = parser.parse_args()

    # Load client secrets for Google Drive API
    creds = service_account.Credentials.from_service_account_file(
        './credentials.json', scopes=['https://www.googleapis.com/auth/drive.metadata.readonly'])

    # Initialize the Google Drive API client
    drive_service = build('drive', 'v3', credentials=creds)

    # Global list to store rows for CSV
    rows_list = []

    # verifying a cache file exists that would hold list of drive files IDs that have been imported already to the given PID
    imported_file_name = f'{args.folder_id}.cache'
    if os.path.exists(imported_file_name) == False:
        Path(imported_file_name).touch()
    
    partner_id = args.partner_id
    admin_secret = args.admin_secret
    category_name = args.root_category_name
    drive_name = args.drive_name
    metadata_profile_id = args.metadata_profile_id

    main(args.folder_id)
