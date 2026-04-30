"""gapi_tools — utilitários para Google Drive e Sheets.

API pública:

* :mod:`gapi_tools.drive` — operações sobre arquivos/pastas.
* :mod:`gapi_tools.sheets` — operações sobre Google Sheets.
* :mod:`gapi_tools.cli` — interface de linha de comando ``gapi``.
"""

from __future__ import annotations

import warnings as _warnings
from typing import Any

from .drive import (
    copy_file,
    create_file,
    create_folder,
    delete_file,
    download_spreadsheet,
    empty_folder,
    find_file_id,
    get_file_modification_time,
    list_files,
    list_files_detailed,
    list_permissions,
    list_revisions,
    move_file,
    rename_file,
    trash_file,
    untrash_file,
    update_metadata,
    upload_file,
)
from .exceptions import (
    ConfigError,
    DriveOperationError,
    GoogleApiUtilsError,
    RemoteFileNotFound,
)
from .sheets import export_dataframe_to_gsheet

__version__ = '0.2.0'

__all__ = [
    '__version__',
    'GoogleApiUtilsError',
    'ConfigError',
    'RemoteFileNotFound',
    'DriveOperationError',
    'list_files',
    'list_files_detailed',
    'find_file_id',
    'download_spreadsheet',
    'upload_file',
    'empty_folder',
    'get_file_modification_time',
    'move_file',
    'create_folder',
    'create_file',
    'copy_file',
    'rename_file',
    'update_metadata',
    'trash_file',
    'untrash_file',
    'delete_file',
    'list_permissions',
    'list_revisions',
    'export_dataframe_to_gsheet',
]


def send_file_to_folder(
    folder_id: str, file_name: str, file_path: str
) -> Any:
    """Alias depreciado de :func:`upload_file`.

    Na assinatura legada, ``file_path`` apontava para a *pasta* contendo
    ``file_name``.
    """
    _warnings.warn(
        'send_file_to_folder está depreciado; use upload_file(folder_id, '
        'file_path, file_name).',
        DeprecationWarning,
        stacklevel=2,
    )
    from pathlib import Path

    return upload_file(folder_id, str(Path(file_path) / file_name), file_name)


def empty_a_folder(folder_id: str) -> bool:
    """Alias depreciado de :func:`empty_folder`."""
    _warnings.warn(
        'empty_a_folder está depreciado; use empty_folder.',
        DeprecationWarning,
        stacklevel=2,
    )
    empty_folder(folder_id)
    return True
