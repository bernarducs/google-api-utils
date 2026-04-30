"""CLI unificada do pacote, exposta como ``gapi``.

As assinaturas dos comandos usam ``typing.Optional`` / ``typing.List`` em vez
de ``X | None`` ou ``list[str]`` porque Typer chama ``typing.get_type_hints``
em runtime e o alvo de execução é Python 3.8.
"""

import json
from typing import List, Optional

import typer

from . import drive, sheets
from .exceptions import GoogleApiUtilsError

app = typer.Typer(
    add_completion=False,
    help='Utilitários para Google Drive e Sheets.',
    no_args_is_help=True,
)
sheets_app = typer.Typer(help='Operações no Google Sheets.', no_args_is_help=True)
app.add_typer(sheets_app, name='sheets')


def _print_json(data) -> None:
    typer.echo(json.dumps(data, indent=2, ensure_ascii=False, default=str))


def _run(callable_, *args, **kwargs):
    """Executa uma operação tratando exceções do pacote como erro de CLI."""
    try:
        return callable_(*args, **kwargs)
    except GoogleApiUtilsError as error:
        typer.secho(str(error), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1)


@app.command('list')
def list_cmd(
    page_size: int = typer.Option(100, '--page-size', '-n'),
    query: Optional[str] = typer.Option(None, '--query', '-q', help='Filtro Drive (campo q).'),
    detailed: bool = typer.Option(False, '--detailed', help='Inclui mimeType e modifiedTime.'),
) -> None:
    """Lista arquivos visíveis pela service account."""
    if detailed or query:
        items = _run(drive.list_files_detailed, query=query, page_size=page_size)
        _print_json(items)
    else:
        _print_json(_run(drive.list_files, page_size=page_size))


@app.command('download')
def download_cmd(
    file_name: str = typer.Argument(..., help='Nome exato do arquivo no Drive.'),
    folder: str = typer.Option('outputs', '--folder', '-f'),
    with_date: bool = typer.Option(False, '--with-date'),
) -> None:
    """Baixa uma planilha (xlsx/xls/Google Sheets)."""
    path = _run(drive.download_spreadsheet, file_name, folder=folder, with_date=with_date)
    typer.echo(str(path))


@app.command('upload')
def upload_cmd(
    file_path: str = typer.Argument(..., help='Caminho local do arquivo.'),
    folder_id: str = typer.Argument(..., help='ID da pasta de destino no Drive.'),
    name: Optional[str] = typer.Option(None, '--name'),
    mimetype: Optional[str] = typer.Option(None, '--mimetype'),
) -> None:
    """Envia um arquivo local para uma pasta do Drive."""
    _print_json(
        _run(drive.upload_file, folder_id, file_path, file_name=name, mimetype=mimetype)
    )


@app.command('move')
def move_cmd(
    file_id: str = typer.Argument(...),
    new_parent_id: str = typer.Argument(...),
    old_parent: Optional[str] = typer.Option(None, '--old-parent'),
) -> None:
    """Move um arquivo entre pastas."""
    _print_json(_run(drive.move_file, file_id, new_parent_id, old_parent_id=old_parent))


@app.command('copy')
def copy_cmd(
    file_id: str = typer.Argument(...),
    name: Optional[str] = typer.Option(None, '--name'),
    parent: Optional[str] = typer.Option(None, '--parent'),
) -> None:
    """Duplica um arquivo."""
    _print_json(_run(drive.copy_file, file_id, new_name=name, parent_id=parent))


@app.command('create-folder')
def create_folder_cmd(
    name: str = typer.Argument(...),
    parent: Optional[str] = typer.Option(None, '--parent'),
) -> None:
    """Cria uma pasta vazia."""
    _print_json(_run(drive.create_folder, name, parent_id=parent))


@app.command('create-file')
def create_file_cmd(
    name: str = typer.Argument(...),
    parent: Optional[str] = typer.Option(None, '--parent'),
    mime_type: str = typer.Option('application/octet-stream', '--mime-type'),
) -> None:
    """Cria um arquivo vazio com o mime-type informado."""
    _print_json(
        _run(drive.create_file, name, parent_id=parent, mime_type=mime_type)
    )


@app.command('rename')
def rename_cmd(
    file_id: str = typer.Argument(...),
    new_name: str = typer.Argument(...),
) -> None:
    """Renomeia um arquivo."""
    _print_json(_run(drive.rename_file, file_id, new_name))


@app.command('update')
def update_cmd(
    file_id: str = typer.Argument(...),
    fields: List[str] = typer.Argument(
        ..., help="Pares chave=valor (ex.: name='Novo' description='ETL diário')."
    ),
) -> None:
    """Atualiza metadados arbitrários (name, description, starred, etc.)."""
    body = {}
    for raw in fields:
        if '=' not in raw:
            typer.secho(f'Argumento inválido: {raw!r} (esperado chave=valor)', err=True, fg=typer.colors.RED)
            raise typer.Exit(code=2)
        key, value = raw.split('=', 1)
        body[key] = value
    _print_json(_run(drive.update_metadata, file_id, **body))


@app.command('trash')
def trash_cmd(file_id: str = typer.Argument(...)) -> None:
    """Move um arquivo para a lixeira."""
    _print_json(_run(drive.trash_file, file_id))


@app.command('untrash')
def untrash_cmd(file_id: str = typer.Argument(...)) -> None:
    """Restaura um arquivo da lixeira."""
    _print_json(_run(drive.untrash_file, file_id))


@app.command('delete')
def delete_cmd(
    file_id: str = typer.Argument(...),
    yes: bool = typer.Option(False, '--yes', '-y', help='Confirma exclusão permanente.'),
) -> None:
    """Apaga um arquivo permanentemente."""
    if not yes:
        typer.confirm(
            f'Confirma exclusão PERMANENTE de {file_id}?', abort=True
        )
    _run(drive.delete_file, file_id)
    typer.echo('ok')


@app.command('empty-folder')
def empty_folder_cmd(
    folder_id: str = typer.Argument(...),
    yes: bool = typer.Option(False, '--yes', '-y'),
) -> None:
    """Apaga todos os arquivos diretamente contidos na pasta."""
    if not yes:
        typer.confirm(
            f'Confirma esvaziamento de {folder_id}?', abort=True
        )
    deleted = _run(drive.empty_folder, folder_id)
    typer.echo(f'arquivos removidos: {deleted}')


@app.command('permissions')
def permissions_cmd(file_id: str = typer.Argument(...)) -> None:
    """Lista as permissões de um arquivo."""
    _print_json(_run(drive.list_permissions, file_id))


@app.command('revisions')
def revisions_cmd(file_id: str = typer.Argument(...)) -> None:
    """Lista o histórico de revisões de um arquivo."""
    _print_json(_run(drive.list_revisions, file_id))


@app.command('mtime')
def mtime_cmd(file_id: str = typer.Argument(...)) -> None:
    """Mostra o ``modifiedTime`` (RFC 3339) de um arquivo."""
    typer.echo(_run(drive.get_file_modification_time, file_id))


@sheets_app.command('export-df')
def export_df_cmd(
    csv_path: str = typer.Argument(..., help='Caminho do CSV a enviar.'),
    gsheet_name: str = typer.Argument(..., help='Nome do Google Sheet de destino.'),
    sheet_name: str = typer.Argument(..., help='Nome da aba dentro do Sheet.'),
    cell: str = typer.Option('A2', '--cell'),
    value_input: str = typer.Option('RAW', '--value-input', help="'RAW' ou 'USER_ENTERED'."),
) -> None:
    """Lê um CSV local e escreve no Google Sheet indicado."""
    import pandas as pd

    df = pd.read_csv(csv_path)
    _print_json(
        _run(
            sheets.export_dataframe_to_gsheet,
            df,
            gsheet_name,
            sheet_name,
            cell_address=cell,
            value_input_option=value_input,
        )
    )


if __name__ == '__main__':
    app()
