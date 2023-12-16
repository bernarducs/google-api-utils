import io
from pathlib import Path
from datetime import datetime
from apiclient import discovery
from dotenv import dotenv_values
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload


ENV = dotenv_values('.env')
TOKEN = Path(Path.home(), ENV['GTOKEN'])
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets',
]


def _create_gdrive_service():
    credentials = Credentials.from_service_account_file(TOKEN, scopes=SCOPES)
    service = discovery.build('drive', 'v3', credentials=credentials)
    return service


def list_files(page_size):
    try:
        service = _create_gdrive_service()
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


def _export_file(file, file_path):
    with open(file_path, 'wb') as f:
        f.write(file.getvalue())
        f.close()


def _download_gsheet_file(file_id, file_path):
    mime_type = (
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    service = _create_gdrive_service()
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
        print(f'An error occurred: {error}')
        file = None

    if file:
        _export_file(file, file_path)


def _download_excel_file(file_id, file_path):
    service = _create_gdrive_service()
    try:
        request = service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f'Download {int(status.progress() * 100)}.')

    except HttpError as error:
        print(f'An error occurred: {error}')
        file = None

    if file:
        _export_file(file, file_path)
        return True
    return False


def _time_now():
    return datetime.now().strftime('%Y%m%d%H%S')


def download_spreadsheet(file_name, dir='outputs/', with_date=False):
    files = list_files(100)
    id_gdrive = files[file_name] if file_name in files else False

    time_now = _time_now() if with_date else ''

    if file_name.endswith('xlsx'):
        _download_excel_file(id_gdrive, f'{dir}{file_name}_{time_now}')
    else:
        _download_gsheet_file(id_gdrive, f'{dir}{file_name}_{time_now}.xlsx')
    print('Download OK.')


def export_dataset(dataframe, gsheet_id, sheet_name, cell_address='A2'):
    service = _create_gdrive_service()
    service_sheet = service.spreadsheets()

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


def send_file_to_folder(folder_id, file_name, file_path):
    """Send a file to a folder in google drive.

    Args:
        folder_id (str): ID of folder in google drive.
        file_name (str): Name of file.
        file_path (str): Path from where the file will be sent.
    """

    service = _create_gdrive_service()
    folder_id = folder_id

    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(Path(file_path, file_name),
                            mimetype='text/plain',
                            resumable=True)
    
    file = service.files().create(body=file_metadata,
                                media_body=media,
                                fields='id').execute()
    