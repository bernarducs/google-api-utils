"""Fixtures compartilhadas — mocks da Google API e isolamento de credenciais."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from google_api_utils import auth


@pytest.fixture
def mock_drive_service(mocker):
    """Substitui ``get_service`` por um MagicMock encadeável.

    Permite asserções como
    ``mock_drive_service.files.return_value.list.return_value.execute.return_value = {...}``.
    """
    auth.reset_service_cache()
    service = MagicMock(name='drive_service')
    mocker.patch('google_api_utils.drive.get_service', return_value=service)
    mocker.patch('google_api_utils.sheets.get_service', return_value=service)
    return service


@pytest.fixture(autouse=True)
def _isolate_credentials(monkeypatch, tmp_path):
    """Garante que nenhum teste tenta ler credenciais reais.

    Aponta ``GOOGLE_API_UTILS_TOKEN`` para um arquivo temporário e mocka
    o ``Credentials.from_service_account_file`` em caminhos que possam
    bypassar o cache.
    """
    fake_token = tmp_path / 'fake_token.json'
    fake_token.write_text('{}')
    monkeypatch.setenv('GOOGLE_API_UTILS_TOKEN', str(fake_token))
    auth.reset_service_cache()
    yield
    auth.reset_service_cache()
