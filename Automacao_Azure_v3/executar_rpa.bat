@echo off
REM Navega para o diretório onde está o ambiente virtualL
cd /d "%~dp0"

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
