"""Testes da interface de linha de comando."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from gapi_tools import cli, drive
from gapi_tools.exceptions import RemoteFileNotFound


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


def test_list_command_calls_list_files(runner, mocker):
    mocker.patch.object(drive, 'list_files', return_value={'a': '1'})
    result = runner.invoke(cli.app, ['list', '--page-size', '10'])
    assert result.exit_code == 0
    assert '"a": "1"' in result.stdout


def test_list_detailed_uses_list_files_detailed(runner, mocker):
    fn = mocker.patch.object(
        drive, 'list_files_detailed', return_value=[{'id': '1', 'name': 'a'}]
    )
    result = runner.invoke(cli.app, ['list', '--detailed'])
    assert result.exit_code == 0
    fn.assert_called_once()


def test_list_with_query_passes_query(runner, mocker):
    fn = mocker.patch.object(drive, 'list_files_detailed', return_value=[])
    runner.invoke(cli.app, ['list', '--query', "name = 'x'"])
    fn.assert_called_once()
    assert fn.call_args.kwargs['query'] == "name = 'x'"


def test_download_prints_path(runner, mocker, tmp_path):
    out_file = tmp_path / 'foo.xlsx'
    mocker.patch.object(drive, 'download_spreadsheet', return_value=out_file)
    result = runner.invoke(cli.app, ['download', 'foo.xlsx'])
    assert result.exit_code == 0
    assert str(out_file) in result.stdout


def test_download_handles_remote_not_found(runner, mocker):
    mocker.patch.object(
        drive, 'download_spreadsheet', side_effect=RemoteFileNotFound('foo')
    )
    result = runner.invoke(cli.app, ['download', 'foo'])
    assert result.exit_code == 1
    assert 'foo' in result.stderr


def test_upload_calls_upload_file(runner, mocker, tmp_path):
    src = tmp_path / 's.txt'
    src.write_text('x')
    fn = mocker.patch.object(drive, 'upload_file', return_value={'id': 'a'})
    result = runner.invoke(cli.app, ['upload', str(src), 'folder123'])
    assert result.exit_code == 0
    fn.assert_called_once()
    args = fn.call_args
    assert args.args == ('folder123', str(src))


def test_move_invokes_move_file(runner, mocker):
    fn = mocker.patch.object(drive, 'move_file', return_value={'id': 'a'})
    result = runner.invoke(cli.app, ['move', 'fid', 'parent', '--old-parent', 'op'])
    assert result.exit_code == 0
    fn.assert_called_once_with('fid', 'parent', old_parent_id='op')


def test_rename_invokes_rename_file(runner, mocker):
    fn = mocker.patch.object(drive, 'rename_file', return_value={'id': 'a'})
    result = runner.invoke(cli.app, ['rename', 'fid', 'novo'])
    assert result.exit_code == 0
    fn.assert_called_once_with('fid', 'novo')


def test_update_parses_key_value_pairs(runner, mocker):
    fn = mocker.patch.object(drive, 'update_metadata', return_value={'id': 'a'})
    result = runner.invoke(
        cli.app,
        ['update', 'fid', 'name=Novo', 'description=Hello'],
    )
    assert result.exit_code == 0
    fn.assert_called_once_with('fid', name='Novo', description='Hello')


def test_update_rejects_malformed_pair(runner):
    result = runner.invoke(cli.app, ['update', 'fid', 'broken'])
    assert result.exit_code == 2
    assert 'broken' in result.stderr


def test_trash_and_untrash(runner, mocker):
    t = mocker.patch.object(drive, 'trash_file', return_value={})
    u = mocker.patch.object(drive, 'untrash_file', return_value={})
    assert runner.invoke(cli.app, ['trash', 'fid']).exit_code == 0
    assert runner.invoke(cli.app, ['untrash', 'fid']).exit_code == 0
    t.assert_called_once_with('fid')
    u.assert_called_once_with('fid')


def test_delete_requires_confirmation(runner, mocker):
    fn = mocker.patch.object(drive, 'delete_file')
    result = runner.invoke(cli.app, ['delete', 'fid'], input='n\n')
    assert result.exit_code != 0
    fn.assert_not_called()


def test_delete_with_yes_skips_prompt(runner, mocker):
    fn = mocker.patch.object(drive, 'delete_file')
    result = runner.invoke(cli.app, ['delete', '--yes', 'fid'])
    assert result.exit_code == 0
    fn.assert_called_once_with('fid')


def test_empty_folder_with_yes(runner, mocker):
    fn = mocker.patch.object(drive, 'empty_folder', return_value=3)
    result = runner.invoke(cli.app, ['empty-folder', '--yes', 'folder'])
    assert result.exit_code == 0
    fn.assert_called_once_with('folder')
    assert 'arquivos removidos: 3' in result.stdout


def test_permissions_outputs_json(runner, mocker):
    mocker.patch.object(
        drive, 'list_permissions', return_value=[{'id': 'p1'}]
    )
    result = runner.invoke(cli.app, ['permissions', 'fid'])
    assert result.exit_code == 0
    assert '"id": "p1"' in result.stdout


def test_revisions_outputs_json(runner, mocker):
    mocker.patch.object(drive, 'list_revisions', return_value=[{'id': 'r1'}])
    result = runner.invoke(cli.app, ['revisions', 'fid'])
    assert result.exit_code == 0
    assert '"id": "r1"' in result.stdout


def test_mtime_prints_string(runner, mocker):
    mocker.patch.object(
        drive, 'get_file_modification_time', return_value='2026-01-01T00:00:00Z'
    )
    result = runner.invoke(cli.app, ['mtime', 'fid'])
    assert result.exit_code == 0
    assert '2026-01-01T00:00:00Z' in result.stdout


def test_sheets_export_df_reads_csv(runner, mocker, tmp_path):
    csv = tmp_path / 'data.csv'
    csv.write_text('a,b\n1,2\n3,4\n')
    fn = mocker.patch(
        'gapi_tools.cli.sheets.export_dataframe_to_gsheet',
        return_value={'updatedRange': 'Plan1!A2:B3'},
    )
    result = runner.invoke(
        cli.app, ['sheets', 'export-df', str(csv), 'gname', 'Plan1']
    )
    assert result.exit_code == 0
    assert fn.called
    args, kwargs = fn.call_args.args, fn.call_args.kwargs
    assert args[1] == 'gname'
    assert args[2] == 'Plan1'
    assert kwargs['cell_address'] == 'A2'
