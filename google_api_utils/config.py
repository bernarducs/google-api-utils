"""Resolução de configuração e credenciais do pacote.

Estratégia de resolução do token (em ordem):
1. Variável de ambiente ``GOOGLE_API_UTILS_TOKEN`` apontando para o JSON da
   service account.
2. Arquivo ``.env`` no diretório de trabalho atual com chave ``GTOKEN``.
3. Arquivo ``.env`` em ``~/.config/google-api-utils/``.

O nome ``GTOKEN`` é interpretado como caminho relativo a ``~`` (home), preservando
o comportamento histórico do projeto.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import dotenv_values

from .exceptions import ConfigError

ENV_VAR_TOKEN = 'GOOGLE_API_UTILS_TOKEN'
ENV_FILE_KEY = 'GTOKEN'

SCOPES = (
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets',
)


def _load_dotenv_token() -> Optional[Path]:
    """Procura ``GTOKEN`` em ``.env`` no cwd e em ``~/.config/google-api-utils/``."""
    candidates = (
        Path.cwd() / '.env',
        Path.home() / '.config' / 'google-api-utils' / '.env',
    )
    for env_path in candidates:
        if not env_path.is_file():
            continue
        values = dotenv_values(env_path)
        token = values.get(ENV_FILE_KEY)
        if token:
            return Path.home() / token
    return None


def resolve_token_path() -> Path:
    """Retorna o ``Path`` do arquivo JSON da service account.

    Raises:
        ConfigError: quando nenhuma fonte fornece um caminho válido.
    """
    env_value = os.environ.get(ENV_VAR_TOKEN)
    if env_value:
        token_path = Path(env_value).expanduser()
    else:
        token_path = _load_dotenv_token()

    if token_path is None:
        raise ConfigError(
            'Token da service account não encontrado. Defina '
            f'{ENV_VAR_TOKEN}=/caminho/para/credenciais.json ou crie um '
            f'.env com {ENV_FILE_KEY}=<arquivo-relativo-ao-home>.'
        )

    if not token_path.is_file():
        raise ConfigError(
            f'Arquivo de credenciais não existe: {token_path}'
        )
    return token_path
