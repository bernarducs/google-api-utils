"""Operações específicas do Google Sheets (API v4)."""

from __future__ import annotations

from typing import Any, Dict, TYPE_CHECKING

from googleapiclient.errors import HttpError

from .auth import get_service
from .drive import find_file_id
from .exceptions import DriveOperationError

if TYPE_CHECKING:
    import pandas as pd


def _sheets():
    return get_service('sheets', 'v4').spreadsheets()


def export_dataframe_to_gsheet(
    dataframe: 'pd.DataFrame',
    gsheet_name: str,
    sheet_name: str,
    cell_address: str = 'A2',
    value_input_option: str = 'RAW',
) -> Dict[str, Any]:
    """Escreve um DataFrame em um Google Sheet existente.

    Args:
        dataframe: DataFrame com os dados.
        gsheet_name: Nome do arquivo Sheets no Drive (deve ser único).
        sheet_name: Nome da aba dentro do arquivo.
        cell_address: Célula inicial (ex.: ``'A2'``).
        value_input_option: ``'RAW'`` ou ``'USER_ENTERED'``.

    Returns:
        Resposta da API (``updatedRange``, ``updatedRows`` etc.).
    """
    gsheet_id = find_file_id(gsheet_name)
    body = {'values': dataframe.values.tolist()}
    range_name = f'{sheet_name}!{cell_address}'
    try:
        return (
            _sheets()
            .values()
            .update(
                spreadsheetId=gsheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body,
            )
            .execute()
        )
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao escrever em {gsheet_name!r}/{sheet_name!r}: {error}'
        ) from error
