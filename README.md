[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/zixaop7v)

# Trabalho Prático 1 - Sistema de Banco de Dados

Este repositório contém os arquivos e scripts necessários para a execução do trabalho prático 1, que envolve o desenvolvimento de um banco de dados baseado no conjunto de dados "Amazon Product Co-Purchasing Network Metadata".

## Descrição dos Arquivos

- **`tp1_3.1.pdf`**: Documento que descreve o desenvolvimento do banco de dados, incluindo modelagem, criação de tabelas, inserção de dados e etapas realizadas no trabalho.
- **`tp1_3.2.py`**: Script Python responsável pela extração dos dados do conjunto "Amazon Product Co-Purchasing Network Metadata". Ele realiza a criação das tabelas no banco de dados e insere os dados extraídos, incluindo todas as relações.
- **`tp1_3.3.py`**: Script Python contendo as principais consultas SQL solicitadas no trabalho. Além disso, implementa a lógica por trás de um dashboard no terminal, permitindo a visualização interativa dos resultados das consultas. Recomenda-se a execução em uma tela grande para melhor visualização.

## Dependências

Os arquivos **`requirements.txt`** e **`install_dependencies.py`** estão incluídos na pasta do projeto para facilitar a configuração do ambiente. Eles garantem que todas as bibliotecas necessárias sejam instaladas de forma organizada e eficiente.

- **`requirements.txt`**: Lista todas as bibliotecas necessárias para o projeto.
- **`install_dependencies.py`**: Script que automatiza a instalação de todas as dependências listadas no `requirements.txt`.

### Instalando Dependências

Para instalar todas as dependências de uma vez, basta executar o seguinte comando:

python3 install_dependencies.py

Se houver algum problema na execução do script, as bibliotecas podem ser instaladas manualmente usando os comandos:

pip install psycopg2

pip install tabulate

## Povoamento do Banco:
python3 tp1_3.2.py

## Execução do script de consultas e dashboard:
python tp1_3.3.py

