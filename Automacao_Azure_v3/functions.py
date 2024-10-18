### INCIANDO O SCRIPT ###
from variables import *
from config import *
import requests
import pandas as pd
from io import BytesIO
from email.mime.text import MIMEText
import smtplib
import os
import shutil
from tqdm import tqdm
import msal
import tabula
from unidecode import unidecode
infos = info()
pd.options.mode.chained_assignment = None  # ignorar mensagens do pandas


data_hora = data_hora_atual(frmt='data_log')
registrar_print("INICIOU O FLOW FUNCTIONS")

# Variaveis de ambiente do Azure DevOps
email = infos['Email']
senha = infos['PasswordEmail']
user_id = infos['user_id']
tenant_id = infos['tenant_id']
client_id = infos['client_id']
client_credential = infos['client_credential']


def token():
    """
    Obtém um token de acesso usando a autenticação do cliente confidencial com o Microsoft Identity Platform.
    Esta função configura um aplicativo confidencial (ConfidentialClientApplication) usando a URL 
    de autoridade do Azure Active Directory com base no ID do locatário 
    (`tenant_id`), o ID do cliente (`client_id`) e a credencial do cliente (`client_credential`). 
    Em seguida, ela solicita um token de acesso para a Microsoft Graph API com escopo padrão.

    Returns:
        token: Um dicionário contendo o token de acesso e outras informações associadas, conforme retornado 
        pela função `acquire_token_for_client` da biblioteca MSAL (Microsoft Authentication Library). O dicionário geralmente inclui a chave 'access_token' com o token de acesso JWT (JSON Web Token).
    """
    authority_url = f'https://login.microsoftonline.com/{tenant_id}'
    app = msal.ConfidentialClientApplication(
        authority=authority_url,
        client_id=client_id,
        client_credential=client_credential
    )
    token = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"])
    return token


def obter_drive_id(user_id):
    """
    Obtém o ID do OneDrive pessoal do usuário especificado.
    Esta função usa o token de acesso obtido pela função `token()` para fazer uma solicitação à Microsoft Graph API, buscando os drives (OneDrive) associados ao usuário com o `user_id` fornecido. Se o usuário tiver drives associados, 
    a função retorna o ID do primeiro drive encontrado. Caso contrário, retorna `None`.

    Args:
        user_id (str): O ID do usuário do qual se deseja obter o ID do drive.
        pasta_id (str or None): O ID do OneDrive pessoal do usuário se encontrado, ou `None` se 
        houver um erro ao obter o token de acesso, se o usuário não tiver drives associados, 
        ou se ocorrer um erro na solicitação à Microsoft Graph API.

    Returns:
        data (str or None): O ID do OneDrive pessoal do usuário se encontrado, ou `None` se houver um erro 
        ao obter o token de acesso, se o usuário não tiver drives associados, ou se ocorrer um erro na 
        solicitação à Microsoft Graph API.
    """
    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return None

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if 'value' in data and len(data['value']) > 0:
            # Obtém o ID do primeiro drive (OneDrive pessoal) do usuário
            drive_id = data['value'][0]['id']
            return drive_id
        else:
            registrar_print("Usuário não tem nenhum drive associado.")
            return None
    else:
        registrar_print("Erro ao obter os drives do usuário.")
        return None


def listar_conteudo_pasta(user_id, pasta_id):
    """
    Lista o conteúdo de uma pasta específica no OneDrive de um usuário.

    Esta função obtém o ID do OneDrive pessoal do usuário usando a função `obter_drive_id`. Em seguida, 
    utiliza um token de acesso obtido pela função `token()` para fazer uma solicitação à Microsoft Graph API 
    para listar o conteúdo da pasta identificada por `pasta_id`. Para cada item na pasta, a função imprime o
    nome e o ID, indicando se o item é uma pasta ou um arquivo.

    Args:
        user_id (str): O ID do usuário cujo OneDrive será acessado.
        pasta_id (str): O ID da pasta cujo conteúdo será listado.

    Returns:
        data (str or None): Um dicionário contendo o conteúdo da pasta se a solicitação for bem-sucedida, ou `None` 
        se houver um erro ao obter o token de acesso, ao obter o ID do drive, ou ao listar o conteúdo da pasta. 
        O dicionário tem a estrutura típica de resposta da API, com a chave 'value' contendo a lista de itens da pasta.
    """
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        return

    token_info = token()
    if not token_info:
        print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}
    url = f"https://graph.microsoft.com/v1.0/users/{
        user_id}/drives/{drive_id}/items/{pasta_id}/children"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        for item in data['value']:
            if 'folder' in item:
                print(f"Pasta: {item['name']} - ID: {item['id']}")
            else:
                print(f"Arquivo: {item['name']} - ID: {item['id']}")
    else:
        print("Erro ao listar o conteúdo da pasta:",
              response.status_code, response.text)
    return data


def ler_arquivo(user_id, arquivo_id, format_, delimitador=None):
    """
    Lê o conteúdo de um arquivo específico no OneDrive de um usuário e o converte em um DataFrame ou lista 
    de DataFrames, dependendo do formato do arquivo.
    Esta função obtém o ID do OneDrive pessoal do usuário usando a função `obter_drive_id`. Em seguida, 
    utiliza um token de acesso obtido pela função `token()` para fazer uma solicitação à Microsoft Graph API
    e obter o conteúdo do arquivo. O conteúdo é processado com base no formato especificado (`format_`). 
    Os formatos suportados são CSV, XLSX, PDF e TXT. 

    Args:
        user_id (str): O ID do usuário cujo OneDrive será acessado.
        arquivo_id (str): O ID do arquivo a ser lido.
        format_ (str): O formato do arquivo. Os formatos suportados são 'csv', 'xlsx', 'pdf', e 'txt'.
        delimitador (str, opcional): O delimitador para arquivos CSV. Por padrão, é `None`.

    Returns:
        pd.DataFrame ou list of pd.DataFrame ou None: 
            - Um DataFrame contendo o conteúdo do arquivo para os formatos 'csv', 'xlsx', ou 'txt'.
            - Uma lista de DataFrames para o formato 'pdf', onde cada DataFrame representa uma tabela extraída do PDF.
            - `None` se ocorrer um erro ao obter o token de acesso, ao obter o ID do drive, ou ao processar o arquivo.
    """
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        return

    token_info = token()
    if not token_info:
        print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}
    url = f"https://graph.microsoft.com/v1.0/users/{
        user_id}/drives/{drive_id}/items/{arquivo_id}/content"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        if format_ == 'csv':
            csv_data = response.content.decode('utf-8')
            df_csv = pd.read_csv(pd.compat.StringIO(
                csv_data), delimiter={delimitador})
            return df_csv
        elif format_ == 'xlsx':
            df_xlsx = pd.read_excel(BytesIO(response.content))
            return df_xlsx
        elif format_ == 'pdf':
            try:
                pdf_data = response.content
                dfs = tabula.read_pdf(
                    pdf_data, pages='all', multiple_tables=True)
                lista_dataframes = []    # Criar uma lista vazia para armazenar os DataFrames
                for i, df in enumerate(dfs, start=1):
                    print(f"Tabela {i}:")
                    print(df)
                    # Adicionar o DataFrame à lista
                    lista_dataframes.append(df)
                return lista_dataframes
            except Exception as e:
                print("Erro ao extrair tabelas do PDF:", e)
                return None
        elif format_ == 'txt':
            txt_data = response.content.decode('utf-8')
            txt_df = pd.read_table(pd.compat.StringIO(txt_data))
            return txt_df
        else:
            print(f"Formato {format_} não suportado.")
            return None
    else:
        print(
            f"Erro ao baixar o arquivo no formato {format_}: {response.status_code}, {response.text}")
        return None


def criar_pasta_log(user_id, pasta_id):
    """
    Cria uma pasta chamada com a data e hora atual dentro de uma pasta especificada no OneDrive do usuário.
    Esta função usa a função `obter_drive_id` para obter o ID do OneDrive pessoal do usuário e um token de acesso pela função `token()`. Em seguida, verifica se já existe uma pasta com o nome correspondente à data e hora atual
    dentro da pasta especificada (`pasta_id`). Se existir, a pasta é excluída. 
    Após a verificação, a função cria uma nova pasta com o nome da data e hora atual.

    Args:
        user_id (str): O ID do usuário cujo OneDrive será acessado.
        pasta_id (str): O ID da pasta onde a nova pasta deve ser criada.

    Returns:
        id_pasta_log (str ou None): O ID da nova pasta criada se a operação for bem-sucedida, ou `None` se houver um erro 
        ao obter o ID do drive, ao obter o token de acesso, ao listar itens da pasta, ao excluir a 
        pasta existente ou ao criar a nova pasta.
    """
    global id_pasta_log
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return None

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return None

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}

    # Verificar se a pasta "log" já existe e removê-la, se for o caso
    url_list_children = f"https://graph.microsoft.com/v1.0/users/{
        user_id}/drives/{drive_id}/items/{pasta_id}/children?$select=id,name"
    response_list_children = requests.get(url_list_children, headers=headers)

    if response_list_children.status_code != 200:
        registrar_print("Erro ao listar itens da pasta pai.")
        return None

    for item in response_list_children.json().get('value', []):
        if item['name'] == data_hora:
            url_delete_folder = f"https://graph.microsoft.com/v1.0/users/{
                user_id}/drives/{drive_id}/items/{item['id']}"
            response_delete_folder = requests.delete(
                url_delete_folder, headers=headers)
            if response_delete_folder.status_code != 204:
                registrar_print("Erro ao excluir a pasta log.")
                return None

    # Criar a pasta "log" dentro da pasta pai com o nome da data e hora local
    url_create_folder = f"https://graph.microsoft.com/v1.0/users/{
        user_id}/drives/{drive_id}/items/{pasta_id}/children"
    data = {
        "name": data_hora,
        "folder": {}
    }
    response_create_folder = requests.post(
        url_create_folder, headers=headers, json=data)

    if response_create_folder.status_code == 201:
        id_pasta_log.clear()
        pasta_log_id = response_create_folder.json().get('id')
        print(f"Pasta 'log' criada com sucesso no OneDrive.")
        return id_pasta_log.append(pasta_log_id)
    else:
        registrar_print("Erro ao criar a pasta log.")
        return None


def criar_log(user_id, pasta_id, lista_prints):
    """
    Cria um arquivo de log no OneDrive do usuário com os registros fornecidos.
    Esta função obtém o ID do OneDrive pessoal do usuário usando a função `obter_drive_id` e um 
    token de acesso pela função `token()`. Em seguida, verifica se já existe um arquivo de log com 
    o nome baseado na data e hora atual. Se o arquivo existir, ele é excluído.
    Depois, um novo arquivo de log é criado e preenchido com os registros fornecidos na lista `lista_prints`.

    Args:
        user_id (str): O ID do usuário cujo OneDrive será acessado.
        pasta_id (str): O ID da pasta onde o arquivo de log deve ser criado.
        lista_prints (list of str): Lista de registros a serem escritos no arquivo de log.

    Returns:
        None: A função não retorna valor. Em vez disso, imprime mensagens de status sobre a 
        criação do arquivo e possíveis erros.
    """
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(
        access_token), 'Content-Type': 'application/json'}

    nome_arquivo = f"{data_hora}.txt"

    # Verificar se o arquivo já existe na pasta pai
    url_list_children = f"https://graph.microsoft.com/v1.0/users/{
        user_id}/drives/{drive_id}/items/{pasta_id}/children?$select=id,name"
    response_list_children = requests.get(url_list_children, headers=headers)

    if response_list_children.status_code == 200:
        for item in response_list_children.json().get('value', []):
            if item['name'] == nome_arquivo:
                # Se o arquivo já existe, deletá-lo
                url_delete_file = f"https://graph.microsoft.com/v1.0/users/{
                    user_id}/drives/{drive_id}/items/{item['id']}"
                response_delete_file = requests.delete(
                    url_delete_file, headers=headers)
                if response_delete_file.status_code == 204:
                    print(f"Arquivo '{nome_arquivo}' existente foi excluído.")
                else:
                    registrar_print("Erro ao excluir o arquivo existente.")

    # Criar o arquivo TXT diretamente na pasta pai com o mesmo nome da data e hora atual
    url = f"https://graph.microsoft.com/v1.0/users/{
        user_id}/drives/{drive_id}/items/{pasta_id}/children"
    data = {
        "name": nome_arquivo,
        "@microsoft.graph.conflictBehavior": "rename",
        "file": {}
    }
    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 201:
        arquivo_id = response.json().get('id')
        url_upload = f"https://graph.microsoft.com/v1.0/users/{
            user_id}/drives/{drive_id}/items/{arquivo_id}/content"

        # Cria o conteúdo do arquivo de texto com os registros da lista de prints
        conteudo_arquivo = '\n'.join(lista_prints) + '\n'

        response_upload = requests.put(
            url_upload, headers=headers, data=conteudo_arquivo.encode('utf-8'))

        if response_upload.status_code == 200:
            print(f"Arquivo '{nome_arquivo}' criado com sucesso no OneDrive.")


def exportar_df(user_id, pasta_id, arquivo, extensao_arquivo, nome):
    """
    Exporta o conteúdo fornecido para um arquivo no OneDrive do usuário.

    Esta função obtém o ID do OneDrive pessoal do usuário usando a função `obter_drive_id` e um token 
    de acesso pela função `token()`. 
    Em seguida, cria um arquivo com o nome e extensão especificados na pasta fornecida (`pasta_id`). 
    O conteúdo do arquivo é o conteúdo 
    fornecido como argumento, convertido para o formato apropriado com base na extensão fornecida. 
    Suporta extensões 'csv' e 'xlsx'.

    Args:
        user_id (str): O ID do usuário cujo OneDrive será acessado.
        pasta_id (str): O ID da pasta onde o arquivo deve ser criado.
        arquivo (str or pd.DataFrame): O conteúdo a ser exportado. Se for uma string, deve ser o conteúdo CSV; se for um DataFrame, será convertido em arquivo.
        extensao_arquivo (str): A extensão do arquivo, pode ser 'csv' ou 'xlsx'.
        nome (str): O nome base do arquivo a ser criado.

    Returns:
        None: A função não retorna valor. Em vez disso, imprime mensagens de status sobre a criação do arquivo e possíveis erros.
    """
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(
        access_token), 'Content-Type': 'application/json'}

    nome_arquivo = f"{nome}.{extensao_arquivo}"

    # Criar o arquivo diretamente na pasta pai com o mesmo nome do DataFrame concatenado com a extensão desejada
    url = f"https://graph.microsoft.com/v1.0/users/{
        user_id}/drives/{drive_id}/items/{pasta_id}/children"
    data = {
        "name": nome_arquivo,
        "@microsoft.graph.conflictBehavior": "rename",
        "file": {}
    }
    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 201:
        arquivo_id = response.json().get('id')
        url_upload = f"https://graph.microsoft.com/v1.0/users/{
            user_id}/drives/{drive_id}/items/{arquivo_id}/content"

        # Convertendo o DataFrame em uma string para enviar ao OneDrive
        conteudo_arquivo = str(arquivo)

        response_upload = requests.put(
            url_upload, headers=headers, data=conteudo_arquivo.encode('utf-8'))

        if response_upload.status_code == 200:
            print(f"Arquivo '{nome_arquivo}' criado com sucesso no OneDrive.")


def baixar_arquivo_online(user_id, pasta_id, url_arquivo, nome_arquivo, extensao_arquivo):
    """
    Faz o download de um arquivo de uma URL e o salva no OneDrive do usuário.

    Esta função obtém o ID do OneDrive pessoal do usuário usando a função `obter_drive_id` e um token de acesso pela função `token()`. 
    Em seguida, faz o download do arquivo da URL fornecida e o cria na pasta especificada (`pasta_id`) no OneDrive. 
    O nome do arquivo é definido pelo argumento `nome_arquivo` e a extensão pelo argumento `extensao_arquivo`.

    Args:
        user_id (str): O ID do usuário cujo OneDrive será acessado.
        pasta_id (str): O ID da pasta onde o arquivo deve ser salvo.
        url_arquivo (str): A URL do arquivo a ser baixado.
        nome_arquivo (str): O nome base do arquivo a ser salvo no OneDrive.
        extensao_arquivo (str): A extensão do arquivo a ser salvo (por exemplo, '.txt', '.pdf').

    Returns:
        None: A função não retorna valor. Em vez disso, imprime mensagens de status sobre a operação e possíveis erros.
    """
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(
        access_token), 'Content-Type': 'application/json'}

    # Fazer o download do arquivo da URL
    response_download = requests.get(url_arquivo)

    if response_download.status_code == 200:
        # Criar o arquivo diretamente na pasta pai com o nome do arquivo baixado
        url = f"https://graph.microsoft.com/v1.0/users/{
            user_id}/drives/{drive_id}/items/{pasta_id}/children"
        data = {
            "name": f'{nome_arquivo}{extensao_arquivo}',
            "@microsoft.graph.conflictBehavior": "rename",
            "file": {}
        }
        response_create = requests.post(url, headers=headers, json=data)

        if response_create.status_code == 201:
            arquivo_id = response_create.json().get('id')
            url_upload = f"https://graph.microsoft.com/v1.0/users/{
                user_id}/drives/{drive_id}/items/{arquivo_id}/content"

            conteudo_arquivo = response_download.content

            response_upload = requests.put(
                url_upload, headers=headers, data=conteudo_arquivo)

            if response_upload.status_code == 200:
                print(
                    f"Arquivo '{nome_arquivo}' baixado e salvo com sucesso no OneDrive.")
            else:
                registrar_print("Erro: Arquivo não foi baixado.")
        else:
            registrar_print("Erro: Arquivo não foi salvo na pasta log")
    else:
        registrar_print("Não foi encontrado a pasta log.")


def remover_acentuacao_titulos(df):
    """
    Remove a acentuação de todos os títulos das colunas do DataFrame.
    Esta função aplica a função `unidecode` para remover acentos e caracteres especiais dos 
    nomes das colunas do DataFrame fornecido.

    Args:
        df (pd.DataFrame): O DataFrame cujo títulos das colunas devem ter a acentuação removida.

    Returns:
        pd.DataFrame: O DataFrame final com os títulos das colunas sem acentuação.
    """
    df = df.rename(columns=lambda x: unidecode(x) if isinstance(x, str) else x)
    return df


def inserir_dados(cursor, table_name, data_frame):
    """
    Insere dados de um DataFrame em uma tabela de banco de dados.
    Esta função insere dados no banco de dados usando uma conexão `cursor` fornecida. 
    O comportamento da inserção varia dependendo do nome da tabela (`table_name`). 
    Se o nome da tabela for 'cbo2002PerfilOcupacional', a função insere dados com um conjunto específico de colunas.
    Caso contrário, assume-se um conjunto diferente de colunas.

    Args:
        cursor (object): O cursor de conexão com o banco de dados.
        table_name (str): O nome da tabela onde os dados serão inseridos.
        data_frame (pd.DataFrame): O DataFrame contendo os dados a serem inseridos.

    Returns:
        None: A função não retorna valor. Em vez disso, realiza a inserção dos dados no banco de dados e imprime mensagens de status.
    """
    if table_name == 'cbo2002PerfilOcupacional':
        query = f"INSERT INTO {table_name} (COD_GRANDE_GRUPO, COD_SUBGRUPO_PRINCIPAL, COD_SUBGRUPO, COD_FAMILIA, \
            COD_OCUPACAO, SGL_GRANDE_AREA, NOME_GRANDE_AREA, COD_ATIVIDADE, NOME_ATIVIDADE, DATA_HORA_CARGA) \
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        data = [
            (
                row['COD_GRANDE_GRUPO'], row['COD_SUBGRUPO_PRINCIPAL'], row['COD_SUBGRUPO'], row['COD_FAMILIA'], row['COD_OCUPACAO'], row[
                    'SGL_GRANDE_AREA'], row['NOME_GRANDE_AREA'], row['COD_ATIVIDADE'], row['NOME_ATIVIDADE'], row['data_hora_carga']
            )
            for _, row in tqdm(data_frame.iterrows(), total=len(data_frame))
        ]
        print(
            f'Iniciou a inserção da {table_name} em:', data_hora_atual(frmt='data_bd'))
        cursor.executemany(query, data)
        print(
            f'Dados na tabela {table_name} injetados com sucesso em:', data_hora_atual(frmt='data_bd'))
    else:
        query = f"INSERT INTO {table_name} (CODIGO, TITULO, DATA_HORA_CARGA) \
                VALUES (?, ?, ?)"
        data = [
            (
                row['CODIGO'], row['TITULO'], row['data_hora_carga']
            )
            for _, row in tqdm(data_frame.iterrows(), total=len(data_frame))
        ]
        print(
            f'Iniciou a inserção da {table_name} em:', data_hora_atual(frmt='data_bd'))
        cursor.executemany(query, data)
        print(
            f'Dados na tabela {table_name} injetados com sucesso em:', data_hora_atual(frmt='data_bd'))


def delete_folder_contents(folder_path):
    """
    Exclui todos os arquivos e subdiretórios dentro da pasta especificada.
    Esta função percorre a pasta especificada e remove todos os arquivos e subdiretórios. 
    Os arquivos são excluídos antes das pastas para evitar problemas com pastas não vazias.

    Args:
        folder_path (str): O caminho para a pasta cuja estrutura de arquivos e subdiretórios deve ser excluída.

    Returns:
        None: A função não retorna valor. Ela apenas imprime mensagens de status para indicar o progresso e os erros.
    """
    for root, dirs, files in os.walk(folder_path, topdown=False):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                os.remove(file_path)
                print(f"Arquivo excluído: {file_path}")
            except Exception as e:
                print(f"Erro ao excluir {file_path}: {e}")

        for dir in dirs:
            dir_path = os.path.join(root, dir)
            try:
                shutil.rmtree(dir_path)
                print(f"Pasta excluída: {dir_path}")
            except Exception as e:
                print(f"Erro ao excluir pasta {dir_path}: {e}")


def enviar_email(lista, rpa):
    """
    Envia um e-mail com um corpo de mensagem baseado em uma lista de strings e um assunto específico.

    Args:
        lista (list of str): Lista de strings a serem incluídas no corpo do e-mail.
        rpa (str): Nome do robô ou sistema que está reportando uma falha.
        servidor_smtp (str): Endereço do servidor SMTP para enviar o e-mail.
        porta_smtp (int): Porta do servidor SMTP.
        email (str): Endereço de e-mail do remetente.
        senha (str): Senha da conta de e-mail do remetente.
        para_email (str): Endereço de e-mail do destinatário.

    Returns:
        None: A função não retorna valor. Ela imprime mensagens de status e erros durante o processo de envio do e-mail.
    """
    # Configurar informações de envio de e-mail
    assunto = f'Falha ao Executar o Robô - {rpa}'
    if senha is None:
        registrar_print(
            'A senha do e-mail não foi configurada nas variáveis de ambiente.')
        return

    # Criar o corpo do e-mail
    corpo_email = '\n'.join(lista)

    try:
        # Configurar a conexão SMTP
        servidor = smtplib.SMTP(servidor_smtp, porta_smtp)
        servidor.starttls()

        # Faça login na conta de e-mail
        servidor.login(email, senha)

        # Crie a mensagem de e-mail
        msg = MIMEText(corpo_email)
        msg['Subject'] = assunto
        msg['From'] = email
        msg['To'] = para_email

        # Envie o e-mail
        servidor.sendmail(email, para_email, msg.as_string())
        servidor.quit()

        registrar_print('E-mail enviado com sucesso!')
    except Exception as e:
        registrar_print(f'Erro ao enviar o e-mail: {str(e)}')


def capturar_id(x, y, z):
    """
    Captura os IDs das pastas "Input" e "Output" dentro de uma estrutura de pastas aninhadas.

    Args:
        user_id (str): ID do usuário do OneDrive.
        x (str): Nome da primeira pasta no nível superior.
        y (str): Nome da segunda pasta dentro da pasta x.
        z (str): Nome da terceira pasta dentro da pasta y.

    Returns:
        id_pasta_input (str): ID da pasta "Input" criada no onedrive.
        id_pasta_output (str): ID da pasta "Output" criada no onedrive.
    """
    global id_pasta_input
    global id_pasta_output
    root = listar_conteudo_pasta(user_id, pasta_id='root')
    if root and 'value' in root:
        for a, item in enumerate(root['value']):
            pasta_a = item.get('name', '')
            if pasta_a == x and 'folder' in item:
                id_pasta_a = item['id']
                pasta_1 = listar_conteudo_pasta(user_id, id_pasta_a)
                if pasta_1 and 'value' in pasta_1:
                    for b, item_b in enumerate(pasta_1['value']):
                        pasta_b = item_b.get('name', '')
                        if pasta_b == y and 'folder' in item_b:
                            id_pasta_b = item_b['id']
                            pasta_2 = listar_conteudo_pasta(
                                user_id, id_pasta_b)
                            if pasta_2 and 'value' in pasta_2:
                                for c, item_c in enumerate(pasta_2['value']):
                                    pasta_c = item_c.get('name', '')
                                    if pasta_c == z and 'folder' in item_c:
                                        id_pasta_c = item_c['id']
                                        pasta_3 = listar_conteudo_pasta(
                                            user_id, id_pasta_c)
                                        if pasta_3 and 'value' in pasta_3:
                                            for d, item_d in enumerate(pasta_3['value']):
                                                pasta_d = item_d.get(
                                                    'name', '')
                                                if pasta_d == 'Input':
                                                    id_pasta_d = item_d['id']
                                                    id_pasta_input.append(
                                                        id_pasta_d)
                                                elif pasta_d == 'Output':
                                                    id_pasta_e = item_d['id']
                                                    id_pasta_output.append(
                                                        id_pasta_e)


def registrar_log(server, base, lista_prints):
    """
    Registra um log na tabela especificada no banco de dados. Se a tabela não existir, ela será criada.

    Args:
        cursor (object): Cursor de conexão com o banco de dados.
        lista_prints (list): Lista contendo informações para registro, onde:
            - lista_prints[0] é o nome do RPA.
            - lista_prints[1] é a data de início.
            - lista_prints[2:] contém a descrição do RPA.
    """
    conn = None
    try:
        # Conectando ao banco de dados
        conn = pyodbc.connect(
            f'Driver=SQL Server;Server={server};Database={base};')

        # Criando um cursor para executar as operações no banco de dados
        cursor = conn.cursor()
        cursor.fast_executemany = True

        table_exists_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{
            tabelaLog}'"
        cursor.execute(table_exists_query)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Criando a tabela tabelaLog caso não exista
            create_table_query = f"""
                    CREATE TABLE [dbo].{tabelaLog}(
                        [vc_nm_nome] [varchar](100) NULL,
                        [vc_ds_descricao] [varchar](5000) NULL,
                        [vc_dt_data_inicio] [varchar](20) NULL,
                        [vc_dt_data_fim] [varchar](20) NULL,
                        [sd_data_hora_carga] [smalldatetime] DEFAULT (getdate()) NULL
                    ) ON [PRIMARY]
                    """
            cursor.execute(create_table_query)
            print(f"Tabela {tabelaLog} criada com sucesso.")
        else:
            print(f"Tabela {tabelaLog} já existe no banco de dados.")

        texto_rpa = '\n'.join(lista_prints[2:])

        # Executar a consulta SQL
        cursor.execute(f"INSERT INTO {tabelaLog} (vc_nm_nome, vc_ds_descricao, vc_dt_data_inicio, vc_dt_data_fim, sd_data_hora_carga) VALUES (?, ?, ?, ?, ?)",
                       (lista_prints[0], texto_rpa, lista_prints[1], data_hora_atual(frmt='data_log'), data_hora_atual(frmt='data_bd')))
        print(f'Log inserido na tabela {tabelaLog} com sucesso.')
        conn.commit()
    except Exception:
        print(f"Erro ao registrar log.")
    finally:
        if conn:
            conn.close()
        print(f"Conexão encerrada da tabela {tabelaLog}.")
