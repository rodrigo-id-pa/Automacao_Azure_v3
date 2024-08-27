### INCIANDO O SCRIPT ###
""" 
bibliotecas padrão necessárias:
'msal', 'traceback', 'requests', 'azure.storage.blob', 'azure.identity', 'tabula', 'smtplib', 'pandas', 'openpyxl'

adicione suas bibliotecas para o desenvolvimento do rpa
"""
from variables import *
import importlib
import subprocess
import datetime
import os
import json


# Obtém o diretório atual
def arquivo_local():
    diretorio_do_arquivo = os.path.dirname(os.path.abspath(__file__))
    print("Diretório do arquivo atual:", diretorio_do_arquivo)
    return diretorio_do_arquivo


# função para ler o json
def load_json(path):
    if os.path.exists(path):
        with open(path, 'r') as arquivo_json:
            return json.load(arquivo_json)
    return None


# função para interar pelos paths e depois chama a função para ler o json
def info():
    path = arquivo_local()
    dados = load_json(path+settings)
    if dados:
        return dados

    print("Caminho para o arquivo JSON não encontrado.")
    return None


# Obter a data e hora local atual no formato "19-07-2023_19-04"
def data_hora_atual(frmt):
    # para criar o logs
    if frmt == 'data_log':
        frmt = datetime.datetime.now()
        frmt = frmt.strftime("%d-%m-%Y_%H-%M")
    # para criar data para o banco
    elif frmt == 'data_bd':
        data_today = datetime.datetime.today().strftime("%A %d %B %y %H:%M")
        frmt = datetime.datetime.strptime(data_today, "%A %d %B %y %H:%M")
    return frmt


# Função para registrar o print na lista de prints
def registrar_print(msg):
    global lista_prints
    print(f'{msg}')  # Exibe o print na saída padrão
    lista_prints.append(msg)  # Adiciona o print à lista


data_ini = data_hora_atual(frmt='data_log')
registrar_print(f'NOME RPA')
registrar_print(f'{data_ini}')
registrar_print('INICIOU A EXECUÇÃO FLOW CONFIG')


# verificando se o pip está atualizado e as bibliotecas de uso deste RPA
def check_install_libs():
    try:
        try:
            import pip
            print('pip já está instalado.')
        except ImportError:
            print('pip não está instalado. Atualizando...')
            subprocess.check_call(
                ['python.exe', '-m', 'pip', 'install', '--upgrade', 'pip'])
            print('pip atualizado com sucesso.')

        # Lista de bibliotecas que você precisa verificar
        bibliotecas = ['msal', 'traceback', 'requests', 'azure-storage-blob', 'azure-identity',
                       'tabula', 'datetime', 'pyodbc', 'io', 'zipfile', 'unidecode', 'selenium', 'time', 'csv', 're', 'smtplib', 'openpyxl', 'tqdm', 'pandas']

        # Verifica se cada biblioteca está instalada e, se necessário, instala
        for biblioteca in bibliotecas:
            try:
                importlib.import_module(biblioteca)
                print(f'{biblioteca} já está instalada.')
            except ImportError:
                print(f'{biblioteca} não está instalada. Instalando...')
                subprocess.check_call(
                    ['pip', 'install', '--upgrade', biblioteca])
                print(f'{biblioteca} instalada com sucesso.')
        registrar_print('FLOW CONFIG REALIZADO COM SUCESSO')
    except Exception as e:
        import traceback
        data_error = data_hora_atual(frmt='data_log')
        traceback_str = traceback.format_exc()
        registrar_print(
            f"Ocorreu um erro:\n{traceback_str}, {data_error}, {e}")


check_install_libs()
