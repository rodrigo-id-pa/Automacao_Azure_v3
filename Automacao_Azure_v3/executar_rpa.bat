@echo off
REM Captura o nome do usuário
set USER_NAME=%USERNAME%

REM Navega para o diretório onde está o ambiente virtualL
cd /d "C:\Users\%USER_NAME%\Documentos\Automacao_Azure_v3"

REM Verifica se o venv existe
IF NOT EXIST "venv\Scripts\activate.bat" (
    echo Ambiente virtual não encontrado. Criando...
    python -m venv venv
)

REM Ativa o ambiente virtual
call venv\Scripts\activate.bat

REM Verifica se o ambiente virtual está ativo
python -c "import sys; print('Usando Python em:', sys.executable)"

REM Executa o script Python
python main.py

REM Ambiente virtual será desativado ao fechar o terminal
