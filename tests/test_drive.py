"""Testes das operações de Drive."""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest
from googleapiclient.errors import HttpError

from google_api_utils import drive
from google_api_utils.exceptions import (
    DriveOperationError,
    RemoteFileNotFound,
)


def _http_error(status: int = 500, reason: str = 'boom') -> HttpError:
    response = MagicMock(status=status, reason=reason)
    return HttpError(resp=response, content=b'{}')


def _set_list_response(service, *pages):
    """Encadeia respostas de ``files().list().execute()`` em ordem."""
    service.files.return_value.list.return_value.execute.side_effect = pages


def test_list_files_paginates_and_returns_name_to_id(mock_drive_service):
    _set_list_response(
        mock_drive_service,
        {
            'files': [{'id': '1', 'name': 'a'}, {'id': '2', 'name': 'b'}],
            'nextPageToken': 'tk',
        },
        {'files': [{'id': '3', 'name': 'c'}]},
    )
    result = drive.list_files(page_size=2)
    assert result == {'a': '1', 'b': '2', 'c': '3'}


def test_list_files_propagates_http_error(mock_drive_service):
    mock_drive_service.files.return_value.list.return_value.execute.side_effect = (
        _http_error()
    )
    with pytest.raises(DriveOperationError):
        drive.list_files()


def test_list_files_detailed_passes_query(mock_drive_service):
    _set_list_response(mock_drive_service, {'files': [{'id': '1', 'name': 'x'}]})
    items = drive.list_files_detailed(query="name = 'x'", page_size=50)
    assert items == [{'id': '1', 'name': 'x'}]
    kwargs = mock_drive_service.files.return_value.list.call_args.kwargs
    assert kwargs['q'] == "name = 'x'"
    assert kwargs['pageSize'] == 50


def test_find_file_id_returns_first_match(mock_drive_service):
    _set_list_response(
        mock_drive_service,
        {'files': [{'id': '42', 'name': 'foo'}]},
    )
    assert drive.find_file_id('foo') == '42'


def test_find_file_id_raises_when_missing(mock_drive_service):
    _set_list_response(mock_drive_service, {'files': []})
    with pytest.raises(RemoteFileNotFound):
        drive.find_file_id('nope')


def test_download_spreadsheet_xlsx_preserves_extension_with_date(
    mock_drive_service, tmp_path, mocker
):
    _set_list_response(
        mock_drive_service,
        {'files': [{'id': 'fid', 'name': 'foo.xlsx'}]},
    )
    captured = {}

    def fake_download(file_id, destination, export):
        captured['destination'] = destination
        captured['export'] = export
        return destination

    mocker.patch.object(drive, '_download_media', side_effect=fake_download)
    mocker.patch.object(drive, '_timestamp_suffix', return_value='20260429_120000')
    out = drive.download_spreadsheet(
        'foo.xlsx', folder=str(tmp_path), with_date=True
    )
    assert out.suffix == '.xlsx'
    assert '20260429_120000' in out.name
    assert out.name == 'foo_20260429_120000.xlsx'
    assert captured['export'] is False


def test_download_spreadsheet_gsheet_exports_xlsx(
    mock_drive_service, tmp_path, mocker
):
    _set_list_response(
        mock_drive_service,
        {'files': [{'id': 'fid', 'name': 'minha-planilha'}]},
    )
    captured = {}

    def fake_download(file_id, destination, export):
        captured['args'] = (file_id, destination, export)
        return destination

    mocker.patch.object(drive, '_download_media', side_effect=fake_download)
    out = drive.download_spreadsheet(
        'minha-planilha', folder=str(tmp_path), with_date=False
    )
    assert out.name == 'minha-planilha.xlsx'
    assert captured['args'][2] is True


def test_upload_file_uses_guessed_mimetype(mock_drive_service, tmp_path):
    src = tmp_path / 'data.csv'
    src.write_text('a,b\n1,2\n')
    mock_drive_service.files.return_value.create.return_value.execute.return_value = {
        'id': 'new'
    }
    result = drive.upload_file('folder123', str(src))
    assert result == {'id': 'new'}
    body = mock_drive_service.files.return_value.create.call_args.kwargs['body']
    assert body == {'name': 'data.csv', 'parents': ['folder123']}


def test_upload_file_raises_when_local_missing(mock_drive_service):
    with pytest.raises(FileNotFoundError):
        drive.upload_file('folder123', '/no/such/file.csv')


def test_empty_folder_paginates_and_counts_deletions(mock_drive_service):
    _set_list_response(
        mock_drive_service,
        {'files': [{'id': '1', 'name': 'a'}, {'id': '2', 'name': 'b'}]},
    )
    mock_drive_service.files.return_value.delete.return_value.execute.return_value = None
    deleted = drive.empty_folder('folder123')
    assert deleted == 2
    delete_calls = mock_drive_service.files.return_value.delete.call_args_list
    assert [c.kwargs['fileId'] for c in delete_calls] == ['1', '2']


def test_get_file_modification_time(mock_drive_service):
    mock_drive_service.files.return_value.get.return_value.execute.return_value = {
        'modifiedTime': '2026-04-29T12:00:00Z'
    }
    assert drive.get_file_modification_time('fid') == '2026-04-29T12:00:00Z'


def test_move_file_resolves_old_parents_when_omitted(mock_drive_service):
    mock_drive_service.files.return_value.get.return_value.execute.return_value = {
        'parents': ['p1', 'p2']
    }
    mock_drive_service.files.return_value.update.return_value.execute.return_value = {
        'id': 'fid',
        'parents': ['p3'],
    }
    drive.move_file('fid', 'p3')
    update_kwargs = mock_drive_service.files.return_value.update.call_args.kwargs
    assert update_kwargs['removeParents'] == 'p1,p2'
    assert update_kwargs['addParents'] == 'p3'


def test_create_folder_sets_correct_mime(mock_drive_service):
    mock_drive_service.files.return_value.create.return_value.execute.return_value = {
        'id': 'fid'
    }
    drive.create_folder('Nova', parent_id='root')
    body = mock_drive_service.files.return_value.create.call_args.kwargs['body']
    assert body == {
        'name': 'Nova',
        'mimeType': drive.FOLDER_MIME,
        'parents': ['root'],
    }


def test_copy_file_passes_only_supplied_fields(mock_drive_service):
    mock_drive_service.files.return_value.copy.return_value.execute.return_value = {
        'id': 'new'
    }
    drive.copy_file('src', new_name='Cópia')
    kwargs = mock_drive_service.files.return_value.copy.call_args.kwargs
    assert kwargs['fileId'] == 'src'
    assert kwargs['body'] == {'name': 'Cópia'}


def test_rename_file_delegates_to_update_metadata(mock_drive_service):
    mock_drive_service.files.return_value.update.return_value.execute.return_value = {
        'id': 'fid',
        'name': 'novo',
    }
    drive.rename_file('fid', 'novo')
    body = mock_drive_service.files.return_value.update.call_args.kwargs['body']
    assert body == {'name': 'novo'}


def test_update_metadata_requires_fields(mock_drive_service):
    with pytest.raises(ValueError):
        drive.update_metadata('fid')


def test_trash_and_untrash_set_flag(mock_drive_service):
    mock_drive_service.files.return_value.update.return_value.execute.return_value = {}
    drive.trash_file('fid')
    drive.untrash_file('fid')
    bodies = [
        c.kwargs['body']
        for c in mock_drive_service.files.return_value.update.call_args_list
    ]
    assert bodies == [{'trashed': True}, {'trashed': False}]


def test_delete_file_calls_delete(mock_drive_service):
    mock_drive_service.files.return_value.delete.return_value.execute.return_value = None
    drive.delete_file('fid')
    assert mock_drive_service.files.return_value.delete.call_args == call(fileId='fid')


def test_list_permissions_returns_list(mock_drive_service):
    mock_drive_service.permissions.return_value.list.return_value.execute.return_value = {
        'permissions': [{'id': 'p1', 'role': 'owner'}]
    }
    perms = drive.list_permissions('fid')
    assert perms == [{'id': 'p1', 'role': 'owner'}]


def test_list_revisions_returns_list(mock_drive_service):
    mock_drive_service.revisions.return_value.list.return_value.execute.return_value = {
        'revisions': [{'id': 'r1'}]
    }
    revs = drive.list_revisions('fid')
    assert revs == [{'id': 'r1'}]


def test_list_revisions_propagates_http_error(mock_drive_service):
    mock_drive_service.revisions.return_value.list.return_value.execute.side_effect = (
        _http_error()
    )
    with pytest.raises(DriveOperationError):
        drive.list_revisions('fid')
