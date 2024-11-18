#!/bin/bash

echo "Configurando o ambiente e iniciando o script..."

# Verifica se o Python está instalado
if ! command -v python3 &> /dev/null
then
    echo "Python não encontrado. Por favor, instale o Python e tente novamente."
    exit 1
fi

# Cria um ambiente virtual
python3 -m venv venv
source venv/bin/activate

# Instala as dependências
pip install -r requirements.txt

# Executa o script Python
python3 main.py

# Desativa o ambiente virtual
deactivate

echo "Script concluído."
read -p "Pressione Enter para sair..."