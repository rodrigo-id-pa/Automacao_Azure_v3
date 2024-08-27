### INCIANDO O SCRIPT ###
from variables import *
from config import *
from functions import *
import traceback
import pandas as pd
import csv
import re
import pyodbc
import zipfile
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import time
infos = info()
pd.options.mode.chained_assignment = None


# Variaveis de ambiente
base = str(infos['BaseBanco'])
server = str(infos['ServidorBanco'])
server_sharepoint = str(infos['ServidorAcesso'])
user_id = str(infos['user_id'])
registrar_print("FLOW FUNCTIONS REALIZADO COM SUCESSO")


try:
    # pegando o id das pastas input e output
    capturar_id(server_sharepoint, 'NOME PASTA', 'NOME PASTA')

    # criando a pasta log
    criar_pasta_log(user_id, pasta_id=id_pasta_output[0])
    registrar_print("INICIOU FLOW MAIN")

    # listar os arquivos e pegar id do csv
    config = listar_conteudo_pasta(user_id, pasta_id=id_pasta_input[0])
    arquivo_id = config['value'][0]['id']

    # Ler csv, xlsx, pdf ou txt - se for csv, passar o delimitador
    path = ler_arquivo(user_id, arquivo_id, format_)
    url = path['Value'][0]

    # Caminho para a pasta de downloads
    caminho_origem = os.path.expanduser('~/Downloads/')

    # Iniciando o navegador modo headless
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()
    driver.get(url)
    time.sleep(3)

    xpath_body = '/html/body'
    if xpath_body:
        element = driver.find_element(By.XPATH, xpath_body).click()

    xpath_cookies = '//button[@aria-label="Aceitar cookies"]'
    if xpath_cookies:
        element = driver.find_element(By.XPATH, xpath_cookies).click()

    # Acessando pagina de download do cbo
    xpath = '//*[@id = "parent-fieldname-text"]/p[1]/a'
    element = driver.find_element(By.XPATH, xpath).click()
    time.sleep(10)
    driver.close()
    registrar_print('EXTRAINDO O ARQUIVO ZIP')

    # Listar todos os arquivos na pasta Downloads
    conteudo_pasta = os.listdir(caminho_origem)

    # Filtrar apenas os arquivos .zip
    arquivos_zip = [
        arquivo for arquivo in conteudo_pasta if arquivo.lower().endswith(".zip")
    ]

    if arquivos_zip:
        nome_arquivo_zip = arquivos_zip[0]
        caminho_arquivo_zip = os.path.join(caminho_origem, nome_arquivo_zip)

        # Pasta de destino para a extração (substitua pelo caminho desejado)
        caminho_destino = os.path.expanduser("~/Downloads/Extracao/")

        # Criar a pasta de destino, se ainda não existir
        os.makedirs(caminho_destino, exist_ok=True)

        # Extrair o arquivo .zip para a pasta de destino
        with zipfile.ZipFile(caminho_arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(caminho_destino)

        registrar_print(
            f"Arquivo '{nome_arquivo_zip}' extraído para '{caminho_destino}'.")
    else:
        registrar_print("Nenhum arquivo .zip encontrado na pasta de origem.")

    # listar os arquivos extraidos
    csv_files = os.listdir(caminho_destino)
    registrar_print('TRATANDO OS DADOS DOS ARQUIVOS CSV')

    # Dicionario com os arquivos csv
    data = {
        'cbo2002Familia': caminho_destino + csv_files[0],
        'cbo2002GrandeGrupo': caminho_destino + csv_files[1],
        'cbo2002Ocupacao': caminho_destino + csv_files[2],
        'cbo2002Sinonimo': caminho_destino + csv_files[4],
        'cbo2002SubGrupoPrincipal': caminho_destino + csv_files[5],
        'cbo2002SubGrupo': caminho_destino + csv_files[6]
    }

    # arquivo csv cbo2002PerfilOcupacional para tratamento de dados
    df_cbo2002PerfilOcupacional = caminho_destino+csv_files[3]

    # abrindo o csv, criando os indices
    with open(df_cbo2002PerfilOcupacional, 'r') as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader):
            list_.append((i, row))

    # percorrer cada indice e remover os ; por / dentro de parenteses
    for i, row in list_:
        for j in range(len(row)):
            # Usando expressão regular para encontrar o padrão "(...)"
            match = re.search(r'\((.*?)\)', row[j])
            if match:
                # Substituindo os pontos e vírgulas por barras apenas dentro dos parênteses
                new_value = re.sub(r';', '/', match.group(1))
                # Atualizando o valor na lista original
                list_[i][1][j] = re.sub(r'\((.*?)\)', f'({new_value})', row[j])

    # percorrendo a nova lista e removendo a linha com "coleta(bags;" pois ele não trocou ; por /
    for item in list_:
        if any("coleta(bags;" in value for value in item[1]):
            list_.remove(item)
            new_index = len(list_)
            new_item = [
                '5;51;519;5192;519205;A;COLETAR MATERIAL RECICLÁVEL E REAPROVEITÁVEL;7;Fornecer recipientes para coleta de bags, conteineres, etc.']
            list_.append((new_index, new_item))

    # Removendo o índice da lista
    df_list = [item[1] for item in list_]
    df = pd.DataFrame(df_list)

    # Juntar todas as colunas em uma única coluna
    df['combined'] = df.apply(lambda row: ' '.join(map(str, row)), axis=1)

    # Dropar as colunas de 0 até 9
    df = df.drop(df.columns[0:10], axis=1)

    # Substituir valores 'None' por espaços em branco na coluna 'combined'
    df['combined'] = df['combined'].replace(r'\bNone\b', ' ', regex=True)

    # Remover espaços em branco extras no final da string
    df['combined'] = df['combined'].str.strip()

    # Separar os dados por ponto e vírgula (;) e criar colunas separadas
    df = df['combined'].str.split(';', expand=True)

    # Definir a primeira coluna como o título
    df.columns = df.iloc[0]

    # Remover a primeira linha (título original)
    cbo2002PerfilOcupacional = df[1:]

    # Coluna com data e hora da execução do bot
    cbo2002PerfilOcupacional['data_hora_carga'] = data_hora_atual(
        frmt='data_bd')

    # Adicionar o dataframe ao dicionário dataframes_
    dataframes_['cbo2002PerfilOcupacional'] = cbo2002PerfilOcupacional

    # percorrendo cada csv, lendo como dataframe e adicionando na lista
    for key, value in data.items():
        df_ = pd.read_csv(value, encoding='cp1252', delimiter=';')
        df_['data_hora_carga'] = data_hora_atual(frmt='data_bd')
        dataframes_[key] = df_
    registrar_print('VERIFICANDO O BANCO DE DADOS')

    # Conectando ao banco de dados
    conn = pyodbc.connect(
        f'Driver=SQL Server;Server={server};Database={base};')

    # Criando um cursor para executar as operações no banco de dados
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # Verificar e criar as tabelas se não existirem
    for tabela, dataframe in dataframes_.items():
        table_exists_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{
            tabela}'"
        cursor.execute(table_exists_query)
        table_exists = cursor.fetchone()[0]
        if table_exists == 0:
            if tabela == 'cbo2002PerfilOcupacional':
                create_table_query = f"""
                            CREATE TABLE [dbo].{tabela}(
                                    [COD_GRANDE_GRUPO] [varchar](100) NULL,
                                    [COD_SUBGRUPO_PRINCIPAL] [varchar](100) NULL,
                                    [COD_SUBGRUPO] [varchar](100) NULL,
                                    [COD_FAMILIA] [varchar](100) NULL,
                                    [COD_OCUPACAO] [varchar](100) NULL,
                                    [SGL_GRANDE_AREA] [varchar](500) NULL,
                                    [NOME_GRANDE_AREA] [varchar](500) NULL,
                                    [COD_ATIVIDADE] [varchar](100) NULL,
                                    [NOME_ATIVIDADE] [varchar](500) NULL,
                                    [DATA_HORA_CARGA] [smalldatetime] NULL
                            ) ON [PRIMARY]
                            ALTER TABLE [dbo].{tabela} ADD DEFAULT (getdate()) FOR [DATA_HORA_CARGA]
                        """
                cursor.execute(create_table_query)
                print(f"Tabela {tabela} criada com sucesso.")
            else:
                create_table_query = f"""
                                CREATE TABLE [dbo].{tabela}(
                                    [CODIGO] [varchar](100) NULL,
                                    [TITULO] [varchar](500) NULL,
                                    [DATA_HORA_CARGA] [smalldatetime] NULL
                                ) ON [PRIMARY]
                                ALTER TABLE [dbo].{tabela} ADD DEFAULT (getdate()) FOR [DATA_HORA_CARGA]
                        """
                cursor.execute(create_table_query)
                print(f"Tabela {tabela} criada com sucesso.")
        else:
            print(f"Tabela {tabela} já existe no banco de dados.")

    # Itera sobre cada tabela na lista
    registrar_print('INSERINDO OS DADOS DOS CSVs NO BANCO DE DADOS')
    for tabela, dataframe in dataframes_.items():
        cursor.execute(f'TRUNCATE TABLE {tabela}')
        inserir_dados(cursor, tabela, dataframe)

    # Exclui o conteúdo da pasta de downloads, incluindo subpastas e arquivos
    delete_folder_contents(caminho_origem)
    registrar_print(f"FINALIZOU O RPA.")
    conn.commit()  # commitando as mudanças

    # Salvando o log no banco de dados, antes de fechar a conexão
    registrar_log(cursor, conn, lista_prints)

    conn.close()  # fechando conexao
    registrar_print("DADOS INJETADOS COM SUCESSO.")

    # criando o txt log dentro da pasta log
    criar_log(user_id, pasta_id=id_pasta_log[0], lista_prints=lista_prints)
except Exception:
    traceback_str = traceback.format_exc()
    registrar_print(
        f"Ocorreu um erro no servidor {server_sharepoint}:\n{traceback_str}")
    enviar_email(lista_prints, lista_prints[0])
    registrar_log(cursor, conn, lista_prints)
    criar_log(user_id, pasta_id=id_pasta_log[0], lista_prints=lista_prints)
