@echo off
echo Configurando o ambiente e iniciando o script...

REM Verifica se o Python está instalado
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python não encontrado. Por favor, instale o Python e tente novamente.
    pause
    exit /b 1
)

REM Cria um ambiente virtual
python -m venv venv
call venv\Scripts\activate

REM Instala as dependências
pip install -r requirements.txt

REM limpa a tela
cls

REM Executa o script Python
python main.py

REM Desativa o ambiente virtual
deactivate

echo Script concluído.
pause