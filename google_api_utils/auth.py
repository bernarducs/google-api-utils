"""Autenticação e cache de serviços Google API.

Usa credenciais de service account (formato JSON) e cacheia o objeto de serviço
por par ``(service_name, version)`` para evitar chamadas redundantes a
``discovery.build`` durante a vida do processo.
"""

from __future__ import annotations

from typing import Dict, Tuple

from googleapiclient import discovery
from googleapiclient.discovery import Resource
from google.oauth2.service_account import Credentials

from .config import SCOPES, resolve_token_path

_service_cache: Dict[Tuple[str, str], Resource] = {}


def _build_credentials() -> Credentials:
    """Carrega as credenciais da service account a partir do token resolvido."""
    token_path = resolve_token_path()
    return Credentials.from_service_account_file(
        str(token_path), scopes=list(SCOPES)
    )


def get_service(service_name: str, version: str) -> Resource:
    """Retorna um cliente Google API, reutilizando instâncias já criadas.

    Args:
        service_name: Nome do serviço (ex.: ``'drive'``, ``'sheets'``).
        version: Versão da API (ex.: ``'v3'``, ``'v4'``).
    """
    key = (service_name, version)
    if key not in _service_cache:
        credentials = _build_credentials()
        _service_cache[key] = discovery.build(
            service_name,
            version,
            credentials=credentials,
            cache_discovery=False,
        )
    return _service_cache[key]


def reset_service_cache() -> None:
    """Limpa o cache interno — útil em testes ou ao trocar credenciais."""
    _service_cache.clear()
