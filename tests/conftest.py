"""Fixtures compartilhadas — mocks da Google API e isolamento de credenciais."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from gapi_tools import auth


@pytest.fixture
def mock_drive_service(mocker):
    """Substitui ``get_service`` por um MagicMock encadeável.

    Permite asserções como
    ``mock_drive_service.files.return_value.list.return_value.execute.return_value = {...}``.
    """
    auth.reset_service_cache()
    service = MagicMock(name='drive_service')
    mocker.patch('gapi_tools.drive.get_service', return_value=service)
    mocker.patch('gapi_tools.sheets.get_service', return_value=service)
    return service


@pytest.fixture(autouse=True)
def _isolate_credentials(monkeypatch, tmp_path):
    """Garante que nenhum teste tenta ler credenciais reais.

    Aponta ``GAPI_TOOLS_TOKEN`` para um arquivo temporário e mocka
    o ``Credentials.from_service_account_file`` em caminhos que possam
    bypassar o cache.
    """
    fake_token = tmp_path / 'fake_token.json'
    fake_token.write_text('{}')
    monkeypatch.setenv('GAPI_TOOLS_TOKEN', str(fake_token))
    auth.reset_service_cache()
    yield
    auth.reset_service_cache()
