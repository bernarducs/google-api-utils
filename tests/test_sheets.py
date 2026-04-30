"""Testes do módulo sheets."""

from __future__ import annotations

import pandas as pd

from gapi_tools import sheets


def test_export_dataframe_to_gsheet_calls_update_with_values(mock_drive_service, mocker):
    mocker.patch(
        'gapi_tools.sheets.find_file_id', return_value='gsheet_id'
    )
    update_chain = (
        mock_drive_service.spreadsheets.return_value.values.return_value.update
    )
    update_chain.return_value.execute.return_value = {
        'updatedRange': "Plan1!A2:B3",
    }

    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    result = sheets.export_dataframe_to_gsheet(df, 'minha-planilha', 'Plan1')

    assert result == {'updatedRange': 'Plan1!A2:B3'}
    kwargs = update_chain.call_args.kwargs
    assert kwargs['spreadsheetId'] == 'gsheet_id'
    assert kwargs['range'] == 'Plan1!A2'
    assert kwargs['valueInputOption'] == 'RAW'
    assert kwargs['body'] == {'values': [[1, 3], [2, 4]]}


def test_export_dataframe_to_gsheet_honors_cell_and_value_input(
    mock_drive_service, mocker
):
    mocker.patch(
        'gapi_tools.sheets.find_file_id', return_value='gid'
    )
    update_chain = (
        mock_drive_service.spreadsheets.return_value.values.return_value.update
    )
    update_chain.return_value.execute.return_value = {}
    df = pd.DataFrame({'x': [1]})
    sheets.export_dataframe_to_gsheet(
        df,
        'g',
        'Sheet1',
        cell_address='C5',
        value_input_option='USER_ENTERED',
    )
    kwargs = update_chain.call_args.kwargs
    assert kwargs['range'] == 'Sheet1!C5'
    assert kwargs['valueInputOption'] == 'USER_ENTERED'
