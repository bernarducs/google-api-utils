"""Operações sobre arquivos e pastas do Google Drive (API v3).

Todas as funções deste módulo levantam :class:`DriveOperationError` ou
:class:`RemoteFileNotFound` em vez de imprimir erros silenciosamente.
"""

from __future__ import annotations

import io
import logging
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from .auth import get_service
from .exceptions import DriveOperationError, RemoteFileNotFound

logger = logging.getLogger(__name__)

GSHEET_EXPORT_MIME = (
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
)
FOLDER_MIME = 'application/vnd.google-apps.folder'
EXCEL_EXTENSIONS = ('.xlsx', '.xls', '.xlsm', '.xlsb')


def _drive():
    return get_service('drive', 'v3')


def _timestamp_suffix() -> str:
    """Retorna timestamp ``YYYYMMDD_HHMMSS`` para uso em nomes de arquivo."""
    return datetime.now().strftime('%Y%m%d_%H%M%S')


def _insert_suffix_before_ext(path: Path, suffix: str) -> Path:
    """Insere ``suffix`` antes da extensão preservando o sufixo original."""
    return path.with_name(f'{path.stem}{suffix}{path.suffix}')


def _iter_file_pages(
    query: Optional[str] = None,
    page_size: int = 100,
    fields: str = 'nextPageToken, files(id, name, mimeType, modifiedTime)',
) -> Iterator[Dict[str, Any]]:
    """Itera todos os arquivos respeitando ``nextPageToken``."""
    service = _drive()
    page_token: Optional[str] = None
    while True:
        params: Dict[str, Any] = {
            'pageSize': page_size,
            'fields': fields,
        }
        if query:
            params['q'] = query
        if page_token:
            params['pageToken'] = page_token
        response = service.files().list(**params).execute()
        for item in response.get('files', []):
            yield item
        page_token = response.get('nextPageToken')
        if not page_token:
            break


def list_files(page_size: int = 100) -> Dict[str, str]:
    """Lista arquivos visíveis e retorna dicionário ``{nome: id}``.

    Mantém compatibilidade com a API legada. Para casos com nomes duplicados,
    prefira :func:`list_files_detailed`.

    Args:
        page_size: Tamanho de página da requisição à API.
    """
    try:
        return {
            item['name']: item['id']
            for item in _iter_file_pages(page_size=page_size)
        }
    except HttpError as error:
        raise DriveOperationError(f'Falha ao listar arquivos: {error}') from error


def list_files_detailed(
    query: Optional[str] = None, page_size: int = 100
) -> List[Dict[str, Any]]:
    """Lista arquivos com metadados completos, suportando query do Drive.

    Args:
        query: Expressão ``q`` da API (ex.: ``"'<folder_id>' in parents"``).
        page_size: Tamanho de página.
    """
    try:
        return list(_iter_file_pages(query=query, page_size=page_size))
    except HttpError as error:
        raise DriveOperationError(f'Falha ao listar arquivos: {error}') from error


def find_file_id(name: str) -> str:
    """Retorna o ``id`` do primeiro arquivo cujo nome bate exatamente.

    Raises:
        RemoteFileNotFound: se nenhum arquivo com esse nome for encontrado.
    """
    safe_name = name.replace("'", "\\'")
    matches = list_files_detailed(query=f"name = '{safe_name}'", page_size=10)
    if not matches:
        raise RemoteFileNotFound(f'Arquivo não encontrado no Drive: {name!r}')
    return matches[0]['id']


def _save_bytes(buffer: io.BytesIO, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(buffer.getvalue())
    return destination


def _download_media(file_id: str, destination: Path, export: bool) -> Path:
    service = _drive()
    try:
        if export:
            request = service.files().export_media(
                fileId=file_id, mimeType=GSHEET_EXPORT_MIME
            )
        else:
            request = service.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.info('Download %d%%.', int(status.progress() * 100))
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao baixar arquivo {file_id!r}: {error}'
        ) from error
    return _save_bytes(buffer, destination)


def download_spreadsheet(
    file_name: str,
    folder: str = 'outputs',
    with_date: bool = False,
) -> Path:
    """Baixa uma planilha (Excel ou Google Sheets) para a pasta indicada.

    Quando o nome termina em ``.xlsx`` / ``.xls`` / ``.xlsm`` / ``.xlsb``, baixa
    o binário diretamente. Caso contrário, exporta um Google Sheet como
    ``.xlsx``. Se ``with_date`` for ``True``, o timestamp é inserido antes da
    extensão (preservando o tipo do arquivo).

    Args:
        file_name: Nome do arquivo no Drive.
        folder: Pasta local de destino.
        with_date: Se ``True``, anexa timestamp ao nome de saída.

    Returns:
        Caminho completo do arquivo gravado em disco.
    """
    file_id = find_file_id(file_name)
    suffix = '_' + _timestamp_suffix() if with_date else ''
    output_dir = Path(folder)

    is_excel_native = file_name.lower().endswith(EXCEL_EXTENSIONS)
    if is_excel_native:
        base = output_dir / file_name
        destination = _insert_suffix_before_ext(base, suffix) if suffix else base
        return _download_media(file_id, destination, export=False)

    base = output_dir / f'{file_name}.xlsx'
    destination = _insert_suffix_before_ext(base, suffix) if suffix else base
    return _download_media(file_id, destination, export=True)


def upload_file(
    folder_id: str,
    file_path: str,
    file_name: Optional[str] = None,
    mimetype: Optional[str] = None,
) -> Dict[str, Any]:
    """Faz upload de um arquivo local para uma pasta do Drive.

    Args:
        folder_id: ID da pasta de destino.
        file_path: Caminho do arquivo local.
        file_name: Nome a usar no Drive. Se omitido, usa o nome do arquivo local.
        mimetype: MIME type. Se omitido, é inferido por
            :func:`mimetypes.guess_type`.
    """
    source = Path(file_path)
    if not source.is_file():
        raise FileNotFoundError(f'Arquivo local não existe: {source}')
    name = file_name or source.name
    guessed, _ = mimetypes.guess_type(str(source))
    media = MediaFileUpload(
        str(source),
        mimetype=mimetype or guessed or 'application/octet-stream',
        resumable=True,
    )
    body = {'name': name, 'parents': [folder_id]}
    try:
        return (
            _drive()
            .files()
            .create(body=body, media_body=media, fields='id, name, parents')
            .execute()
        )
    except HttpError as error:
        raise DriveOperationError(f'Falha no upload: {error}') from error


def empty_folder(folder_id: str) -> int:
    """Apaga todos os arquivos diretamente contidos em ``folder_id``.

    Returns:
        Quantidade de arquivos removidos.
    """
    deleted = 0
    service = _drive()
    try:
        files = list_files_detailed(
            query=f"'{folder_id}' in parents", page_size=1000
        )
        for item in files:
            service.files().delete(fileId=item['id']).execute()
            deleted += 1
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao esvaziar pasta {folder_id!r}: {error}'
        ) from error
    return deleted


def get_file_modification_time(file_id: str) -> str:
    """Retorna o ``modifiedTime`` (RFC 3339) do arquivo."""
    try:
        file = (
            _drive()
            .files()
            .get(fileId=file_id, fields='modifiedTime')
            .execute()
        )
        return file['modifiedTime']
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao obter modifiedTime de {file_id!r}: {error}'
        ) from error


def move_file(
    file_id: str,
    new_parent_id: str,
    old_parent_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Move um arquivo para outra pasta.

    Se ``old_parent_id`` não for informado, todos os parents atuais são
    removidos e substituídos pelo novo.
    """
    service = _drive()
    try:
        if old_parent_id is None:
            current = (
                service.files().get(fileId=file_id, fields='parents').execute()
            )
            old_parents = ','.join(current.get('parents', []))
        else:
            old_parents = old_parent_id
        return (
            service.files()
            .update(
                fileId=file_id,
                addParents=new_parent_id,
                removeParents=old_parents,
                fields='id, parents',
            )
            .execute()
        )
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao mover {file_id!r}: {error}'
        ) from error


def create_folder(
    name: str, parent_id: Optional[str] = None
) -> Dict[str, Any]:
    """Cria uma pasta vazia no Drive."""
    body: Dict[str, Any] = {'name': name, 'mimeType': FOLDER_MIME}
    if parent_id:
        body['parents'] = [parent_id]
    try:
        return (
            _drive()
            .files()
            .create(body=body, fields='id, name, parents')
            .execute()
        )
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao criar pasta {name!r}: {error}'
        ) from error


def create_file(
    name: str,
    parent_id: Optional[str] = None,
    mime_type: str = 'application/octet-stream',
    content: bytes = b'',
) -> Dict[str, Any]:
    """Cria um arquivo (vazio ou com conteúdo) diretamente no Drive."""
    body: Dict[str, Any] = {'name': name, 'mimeType': mime_type}
    if parent_id:
        body['parents'] = [parent_id]
    media = None
    if content:
        from googleapiclient.http import MediaIoBaseUpload

        media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime_type)
    try:
        request = _drive().files().create(
            body=body,
            media_body=media,
            fields='id, name, parents, mimeType',
        )
        return request.execute()
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao criar arquivo {name!r}: {error}'
        ) from error


def copy_file(
    file_id: str,
    new_name: Optional[str] = None,
    parent_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Duplica um arquivo, opcionalmente renomeando e/ou movendo."""
    body: Dict[str, Any] = {}
    if new_name:
        body['name'] = new_name
    if parent_id:
        body['parents'] = [parent_id]
    try:
        return (
            _drive()
            .files()
            .copy(fileId=file_id, body=body, fields='id, name, parents')
            .execute()
        )
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao copiar {file_id!r}: {error}'
        ) from error


def rename_file(file_id: str, new_name: str) -> Dict[str, Any]:
    """Renomeia um arquivo (atalho de :func:`update_metadata`)."""
    return update_metadata(file_id, name=new_name)


def update_metadata(file_id: str, **fields: Any) -> Dict[str, Any]:
    """Atualiza metadados editáveis (``name``, ``description``, ``starred``...)."""
    if not fields:
        raise ValueError('Forneça pelo menos um campo para atualizar.')
    try:
        return (
            _drive()
            .files()
            .update(
                fileId=file_id,
                body=dict(fields),
                fields='id, name, description',
            )
            .execute()
        )
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao atualizar metadados de {file_id!r}: {error}'
        ) from error


def trash_file(file_id: str) -> Dict[str, Any]:
    """Move o arquivo para a lixeira do Drive."""
    return update_metadata(file_id, trashed=True)


def untrash_file(file_id: str) -> Dict[str, Any]:
    """Restaura o arquivo da lixeira."""
    return update_metadata(file_id, trashed=False)


def delete_file(file_id: str) -> None:
    """Apaga um arquivo permanentemente (ignora a lixeira)."""
    try:
        _drive().files().delete(fileId=file_id).execute()
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao apagar {file_id!r}: {error}'
        ) from error


def list_permissions(file_id: str) -> List[Dict[str, Any]]:
    """Lista as permissões (quem tem acesso) de um arquivo."""
    try:
        response = (
            _drive()
            .permissions()
            .list(
                fileId=file_id,
                fields='permissions(id, type, role, emailAddress, displayName)',
            )
            .execute()
        )
        return response.get('permissions', [])
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao listar permissões de {file_id!r}: {error}'
        ) from error


def list_revisions(file_id: str) -> List[Dict[str, Any]]:
    """Retorna o histórico de revisões de um arquivo."""
    try:
        response = (
            _drive()
            .revisions()
            .list(
                fileId=file_id,
                fields='revisions(id, modifiedTime, lastModifyingUser, size)',
            )
            .execute()
        )
        return response.get('revisions', [])
    except HttpError as error:
        raise DriveOperationError(
            f'Falha ao listar revisões de {file_id!r}: {error}'
        ) from error
