"""Exceções tipadas do pacote google_api_utils."""

from __future__ import annotations


class GoogleApiUtilsError(Exception):
    """Erro base do pacote."""


class ConfigError(GoogleApiUtilsError):
    """Configuração ausente ou inválida (token, .env, etc.)."""


class RemoteFileNotFound(GoogleApiUtilsError):
    """Arquivo procurado não existe no Google Drive."""


class DriveOperationError(GoogleApiUtilsError):
    """Falha durante uma chamada à API do Drive/Sheets."""
