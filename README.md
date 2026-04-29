# google-api-utils

Utilitários em Python para automatizar tarefas no Google Drive (v3) e Google Sheets (v4) via *service account*. Inclui uma CLI unificada (`gapi`) e uma API programática.

## Funcionalidades

- Listar arquivos (resumido ou detalhado) e localizar por nome.
- Download de planilhas (preserva extensão `.xlsx`/`.xls`/`.xlsm`/`.xlsb` ou exporta Google Sheets como `.xlsx`).
- Upload de arquivos com *mimetype* detectado automaticamente.
- Mover, copiar, renomear, criar pasta/arquivo.
- Atualizar metadados, mandar para a lixeira, restaurar, apagar.
- Esvaziar pasta (com paginação).
- Listar permissões e revisões.
- Ler/escrever planilhas a partir de um `pandas.DataFrame`.

## Requisitos

- Python **3.8.x** (cap por compatibilidade com o servidor de produção; veja seção abaixo).
- Service account no Google Cloud com APIs **Drive** e **Sheets** habilitadas e arquivo JSON de credenciais.

## Instalação

Em ambiente de desenvolvimento (Linux/WSL):

```bash
python3.8 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Em produção:

```bash
pip install .
```

Após instalar, o comando `gapi` fica disponível no `PATH`.

## Configuração

Aponte para o JSON da service account de uma das três formas (resolvidas nessa ordem):

1. variável de ambiente `GOOGLE_API_UTILS_TOKEN` com o caminho absoluto do arquivo JSON;
2. arquivo `.env` no diretório atual contendo `GTOKEN=/caminho/para/credenciais.json`;
3. arquivo `.env` em `~/.config/google-api-utils/`.

A planilha/arquivo precisa estar **compartilhado** com o e-mail da service account.

## Uso da CLI

```text
gapi list                                         # nome -> id
gapi list --detailed --query "name = 'foo'"       # JSON com metadados
gapi download minha-planilha --folder ./out --with-date
gapi upload ./local.csv FOLDER_ID --name remoto.csv
gapi move FILE_ID NEW_PARENT_ID --old-parent OLD
gapi copy FILE_ID --name "Cópia"
gapi create-folder Nova --parent root
gapi rename FILE_ID novo-nome
gapi update FILE_ID name=Foo description=Bar
gapi trash FILE_ID
gapi untrash FILE_ID
gapi delete FILE_ID --yes
gapi empty-folder FOLDER_ID --yes
gapi permissions FILE_ID
gapi revisions FILE_ID
gapi mtime FILE_ID
gapi sheets export-df dados.csv minha-planilha Plan1 --cell A2
```

`gapi --help` mostra todos os comandos; cada subcomando tem `--help` próprio.

## Uso programático

```python
from google_api_utils import drive, sheets
import pandas as pd

# listar e baixar
files = drive.list_files_detailed(query="mimeType = 'application/vnd.google-apps.spreadsheet'")
out = drive.download_spreadsheet('minha-planilha', folder='./out', with_date=True)

# upload e organização
drive.upload_file('FOLDER_ID', './relatorio.xlsx')
drive.move_file('FILE_ID', 'NEW_PARENT_ID')
drive.rename_file('FILE_ID', 'novo-nome.xlsx')

# escrever DataFrame numa aba
df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
sheets.export_dataframe_to_gsheet(df, 'minha-planilha', 'Plan1', cell_address='A2')
```

## Setup do servidor Windows Server 2008 R2

Este pacote é distribuído para um Windows Server 2008 R2 com Python **3.8.1**. Antes de instalar:

1. **Atualizações** — recomendado aplicar `KB3140245` ("Update to enable TLS 1.1 and TLS 1.2 as default secure protocols in WinHTTP").
2. **Habilitar TLS 1.2** no registro (chamadas à API do Google falham silenciosamente sem isso). Em `regedit`, criar/editar:

   ```
   HKLM\SYSTEM\CurrentControlSet\Control\SecurityProviders\SCHANNEL\Protocols\TLS 1.2\Client
       Enabled            = 1   (DWORD)
       DisabledByDefault  = 0   (DWORD)
   HKLM\SYSTEM\CurrentControlSet\Control\SecurityProviders\SCHANNEL\Protocols\TLS 1.2\Server
       Enabled            = 1   (DWORD)
       DisabledByDefault  = 0   (DWORD)
   ```

   Reiniciar após alterar.
3. **Verificar TLS** — após reiniciar:

   ```
   python -c "import urllib.request; print(urllib.request.urlopen('https://www.googleapis.com').status)"
   ```

   Se falhar com erro de SSL, TLS 1.2 não está ativo.
4. **Instalar o pacote** — todas as dependências têm wheels `cp38-win_amd64`, então não é necessário compilador:

   ```
   pip install .
   ```
5. **Sanity check** — `gapi --help` deve listar todos os subcomandos sem `ImportError`.

> Nota: o pacote evita `rich` (usa `typer` puro, não `typer[all]`) para funcionar bem no `cmd.exe` legado em cp1252.

## Testes

```bash
pytest -v
```

Todos os testes usam mocks da Google API e não dependem de rede ou credenciais reais.

## Lint e formatação

```bash
ruff check .
ruff format .
```

## Licença

MIT — veja [LICENSE](LICENSE).
