"""utils.py

Arquivo que lida com diversas tarefas do google drive e sheets.
"""

import io
from datetime import datetime
from pathlib import Path

from apiclient import discovery
from dotenv import dotenv_values
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


ENV = dotenv_values(Path(Path.cwd(), '.env'))
TOKEN = Path(Path.home(), ENV['GTOKEN'])
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets',
]


def _create_gdrive_service(service_name: str, version: str):
    credentials = Credentials.from_service_account_file(TOKEN, scopes=SCOPES)
    service = discovery.build(
        service_name, version, credentials=credentials, cache_discovery=False
    )
    return service


def list_files(page_size: int):
    try:
        service = _create_gdrive_service('drive', 'v3')
        results = (
            service.files()
            .list(pageSize=page_size, fields='nextPageToken, files(id, name)')
            .execute()
        )
        items = results.get('files', [])

        if items:
            return {item['name']: item['id'] for item in items}
        else:
            print('No files found.')
            return {}
    except HttpError as error:
        print(f'An error occurred: {error}')


def _export_file(file, file_path: str):
    with open(file_path, 'wb') as f:
        f.write(file.getvalue())
        f.close()


def _download_gsheet_file(file_id: str, file_path: str):
    mime_type = (
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    service = _create_gdrive_service('drive', 'v3')
    try:
        request = service.files().export_media(
            fileId=file_id, mimeType=mime_type
        )
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False

        while done is False:
            status, done = downloader.next_chunk()
            print(f'Download {int(status.progress() * 100)}.')

    except HttpError as error:
        return error

    if file:
        _export_file(file, file_path)
        return True


def _download_file(file_id, file_path):
    service = _create_gdrive_service('drive', 'v3')
    try:
        request = service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False

        while done is False:
            status, done = downloader.next_chunk()
            print(f'Download {int(status.progress() * 100)}.')

    except HttpError as error:
        return error

    if file:
        _export_file(file, file_path)
        return True


def _time_now():
    return datetime.now().strftime('%Y%m%d%H%S')


def download_spreadsheet(
    file_name: str, folder: str = 'outputs', with_date: bool = False
):
    files = list_files(100)
    id_gdrive = files[file_name] if file_name in files else False

    time_now = '_' + _time_now() if with_date else ''

    output_folder = Path(folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    if file_name.endswith('xlsx') or file_name.endswith('xls'):
        return _download_file(
            id_gdrive, Path(output_folder, f'{file_name}{time_now}')
        )
    else:
        return _download_gsheet_file(
            id_gdrive, Path(output_folder, f'{file_name}{time_now}.xlsx')
        )


def export_dataframe_to_gsheet(
    dataframe, gsheet_name: str, sheet_name: str, cell_address: str = 'A2'
):
    service = _create_gdrive_service('sheets', 'v4')
    service_sheet = service.spreadsheets()

    files = list_files(200)
    gsheet_id = files[gsheet_name]

    values = dataframe.values.tolist()
    body = {'values': values}
    range_name = f'{sheet_name}!{cell_address}'

    result = (
        service_sheet.values()
        .update(
            spreadsheetId=gsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body,
        )
        .execute()
    )
    return result


def send_file_to_folder(folder_id: str, file_name: str, file_path: str):
    """Send a file to a folder in google drive.

    Args:
        folder_id (str): ID of folder in google drive.
        file_name (str): Name of file.
        file_path (str): Path from where the file will be sent.
    """

    service = _create_gdrive_service('drive', 'v3')
    folder_id = folder_id

    file_metadata = {'name': file_name, 'parents': [folder_id]}
    media = MediaFileUpload(
        Path(file_path, file_name), mimetype='text/plain', resumable=True
    )

    file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields='id')
        .execute()
    )
    return file


def empty_a_folder(folder_id: str):
    """Delete all files in folder.

    Args:
        folder_id (str): The folder's ID.

    Returns:
        bool: True if succeeds.
    """
    service = _create_gdrive_service('drive', 'v3')
    results = (
        service.files()
        .list(
            q=f"'{folder_id}' in parents",
            pageSize=1000,
            fields='nextPageToken, files(id, name)',
        )
        .execute()
    )

    items = results.get('files', [])

    for file in items:
        try:
            service.files().delete(fileId=file['id']).execute()
        except HttpError as error:
            print(f'An error occurred: {error}')
            return False
    return True


def get_file_modification_time(file_id):
    """Returns file modification date.

    Args:
        file_id (str): The file's ID.

    Returns:
        str: The file modification date.
    """
    try:
        service = _create_gdrive_service('drive', 'v3')
        file = (
            service.files()
            .get(fileId=file_id, fields='modifiedTime')
            .execute()
        )
        return file.get('modifiedTime')
    except HttpError as error:
        print(f'An error occurred: {error}')
