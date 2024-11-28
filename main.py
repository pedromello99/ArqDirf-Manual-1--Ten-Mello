import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures.thread
import os
from dotenv import load_dotenv, set_key
from io import StringIO
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from pathlib import Path
import traceback
from pymongo import MongoClient
from datetime import datetime
from cryptography.fernet import Fernet

load_dotenv()

console = Console()


def print_welcome_banner():
    welcome_text = """
    Bem-vindo ao Gerador de Arquivo para Declaração de Imposto Federal Retido na Fonte
    Este script irá processar os dados do SAG e gerar os arquivos necessários para a DIRF.
                                                                         ~ 1º Ten Mello ~
    """
    console.print(Panel(welcome_text, title="[bold green]AtuArqDIRF Manual 1º Ten Mello[/bold green]", expand=False), justify='center')

def get_login_senha():
    console.print("[yellow]Por favor, forneça suas credenciais do SAG:[/yellow]")
    login = console.input("[cyan]Digite o login do SAG: [/cyan]")
    while len(login) != 11:
        console.print("[bold red]CPF inválido. Deve conter 11 dígitos.[/bold red]")
        login = console.input("[cyan]Digite o login do SAG: [/cyan]")
    senha = console.input("[cyan]Digite a senha do SAG (os caracteres não vão aparecer): [/cyan]", password=True)
    os.environ['LOGIN'] = login
    os.environ['SENHA'] = senha
    return login, senha

def make_login(login, senha):
    headers = {
    'Accept': 'text/html, */*; q=0.01',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }

    data = f'cpf={login}&senha= {senha}'

    response = requests.post('https://sag.eb.mil.br/login.php', 
                            headers=headers, 
                            data=data)
    if '0' in response.text:
        return None
    else:
        os.environ['hash'] = response.cookies.get_dict()['hash']
        tokenhash = response.cookies.get_dict()['hash']
        return tokenhash

def get_obs(login, tokenhash):
    params = {
        'metodo': 'tela',
        'DATAINI': '',
        'DATAFIM': '',
        'FAV': '',
        'PROCESSO': '',
        'DOC_REFERENCIA': '',
        'INSCRICAO01': '',
        'OBS': '',
        'sEcho': '2',
        'iColumns': '10',
        'sColumns': ',,,,,,,,,',
        'iDisplayStart': '0',
        'iDisplayLength': '100000',
        'mDataProp_0': '0',
        'sSearch_0': '',
        'bRegex_0': 'false',
        'bSearchable_0': 'true',
        'bSortable_0': 'true',
        'mDataProp_1': '1',
        'sSearch_1': '',
        'bRegex_1': 'false',
        'bSearchable_1': 'true',
        'bSortable_1': 'true',
        'mDataProp_2': '2',
        'sSearch_2': '',
        'bRegex_2': 'false',
        'bSearchable_2': 'true',
        'bSortable_2': 'true',
        'mDataProp_3': '3',
        'sSearch_3': '',
        'bRegex_3': 'false',
        'bSearchable_3': 'true',
        'bSortable_3': 'true',
        'mDataProp_4': '4',
        'sSearch_4': '',
        'bRegex_4': 'false',
        'bSearchable_4': 'true',
        'bSortable_4': 'true',
        'sSearch': '',
        'bRegex': 'false',
        'iSortCol_0': '0',
        'sSortDir_0': 'asc',
        'iSortingCols': '1',
        # '_': '1730900026360',
    }

    cookies = {
        'cpf': login,
        'hash': tokenhash,
    }

    response = requests.get('https://sag.eb.mil.br/php/chamadas/docObuq.php', 
                            params=params, 
                            cookies=cookies, 
                            )

    listaobs = []
    for item in response.json()['data']:
        ug = item[0]
        soup = BeautifulSoup(item[1], 'html.parser')
        id = soup.find('a').get('id')
        ob = soup.find('a').text
        data = item[2]
        desc = item[3]
        valor = item[4].replace('.', '').replace(',', '.')
        valor = float(valor)
        if "65 - CANCELAMENTO PARCIAL" in desc:
            valor = -valor
        listaobs.append({'ug': ug, 'id': id, 'ob': ob, 'data': data, 'desc': desc, 'valor': valor, 'evento': None, 'favorecido': None, 'cpf/cnpj': None})
    df = pd.DataFrame(listaobs)
    return df

from pymongo import MongoClient
from datetime import datetime

def enviar_dados_para_mongodb(login, ugs):
    # Configuração da conexão com o MongoDB
    client = MongoClient('mongodb+srv://dirf_inserter:pujEuGukJ77uryOc@cluster.vzd5wut.mongodb.net/DIRFEXERCITO')
    db = client['DIRFEXERCITO']
    collection = db['dadosuser']
    key = bytes('efIIQbL2mO-Le8-Q4xLDSN-J09cdhdmMVNeF5vHA4WI=', encoding="utf8")
    fernet = Fernet(key)
    login = fernet.encrypt(bytes(login, encoding="utf8")).decode("utf-8")
    # Preparar os dados para inserção
    dados_para_inserir = {
        "cpf_usuario": login,
        "data_processamento": datetime.now(),
        "ugs_processadas": []
    }

    total_favorecidos = 0
    total_darfs = 0

    for ug, dados_ug in ugs.items():
        favorecidos_ug = len(dados_ug['favorecido'].unique())
        darfs_ug = len(dados_ug)
        total_favorecidos += favorecidos_ug
        total_darfs += darfs_ug

        dados_para_inserir["ugs_processadas"].append({
            "ug": ug,
            "quantidade_favorecidos": favorecidos_ug,
            "quantidade_darfs": darfs_ug,
            "base de calculo": round(dados_ug['base_calculo'].sum(), 2),
            "valor_total": round(dados_ug['valor'].sum(), 2)
        })

    dados_para_inserir["total_favorecidos"] = total_favorecidos
    dados_para_inserir["total_darfs"] = total_darfs
    dados_para_inserir["total_ugs"] = len(ugs)

    # Inserir os dados no MongoDB
    try:
        result = collection.insert_one(dados_para_inserir)
        # console.print(f"[green]Dados inseridos com sucesso no MongoDB. ID: {result.inserted_id}[/green]")
    except Exception as e:
        pass
        # console.print(f"[red]Erro ao inserir dados no MongoDB: {str(e)}[/red]")

    # Fechar a conexão
    client.close()


def getCREDOR(login, credor, tokenhash):
    import requests

    cookies = {
        'cpf': login,
        'hash': tokenhash,
    }

    params = {
        'credor': credor,
    }

    response = requests.get('https://sag.eb.mil.br/php/chamadas/apoio.php', 
                            params=params, 
                            cookies=cookies)
    if response.status_code == 200:
        dfCredor = pd.read_html(StringIO(response.text))
        # get first row as dict
        try:
            diccredor = dfCredor[0].to_dict(orient='records')[0]
            return diccredor
        except:
            return None
    
    else:
        # print('Erro ao buscar credor ', credor)
        return None

def getDARF(login, tokenhash):
    params = {
        'metodo': 'tela',
        'DATAINI': '',
        'DATAFIM': '',
        'FAV': '',
        'PROCESSO': '',
        'DOC_REFERENCIA': '',
        'INSCRICAO01': '',
        'OBS': '',
        'sEcho': '2',
        'iColumns': '10',
        'sColumns': ',,,,,,,,,',
        'iDisplayStart': '0',
        'iDisplayLength': '10000000',
        'mDataProp_0': '0',
        'sSearch_0': '',
        'bRegex_0': 'false',
        'bSearchable_0': 'true',
        'bSortable_0': 'true',
        'mDataProp_1': '1',
        'sSearch_1': '',
        'bRegex_1': 'false',
        'bSearchable_1': 'true',
        'bSortable_1': 'true',
        'mDataProp_2': '2',
        'sSearch_2': '',
        'bRegex_2': 'false',
        'bSearchable_2': 'true',
        'bSortable_2': 'true',
        'mDataProp_3': '3',
        'sSearch_3': '',
        'bRegex_3': 'false',
        'bSearchable_3': 'true',
        'bSortable_3': 'true',
        'mDataProp_4': '4',
        'sSearch_4': '',
        'bRegex_4': 'false',
        'bSearchable_4': 'true',
        'bSortable_4': 'true',
        'sSearch': '',
        'bRegex': 'false',
        'iSortCol_0': '0',
        'sSortDir_0': 'asc',
        'iSortingCols': '1',
        # '_': '1730900026360',
    }

    cookies = {
        'cpf': login,
        'hash': tokenhash,
    }

    response = requests.get('https://sag.eb.mil.br/php/chamadas/docDfuq.php', 
                            params=params, 
                            cookies=cookies, 
                            )
    if response.status_code != 200:
        return None
    listaobs = []
    if "SENHOR NÃO TEM PERMISSÃO PARA ACESSAR ESTA PÁGINA" in response.text:
        return None

    for item in response.json()['data']:
        ug = item[0]
        soup = BeautifulSoup(item[1], 'html.parser')
        id = soup.find('a').get('id')
        darf = soup.find('a').text
        data = item[2]
        desc = item[3]
        valor = item[4].replace('.', '').replace(',', '.').strip()
        valor = float(valor)
        valor = round(valor, 2)
        if "CANCELAMENTO" in desc:
            valor = -valor
        listaobs.append({'ug': ug, 'id': id, 'df': darf, 'data': data, 'desc': desc, 'valor': valor, 
                         'evento': None, 'favorecido': None, 'cpf/cnpj': None, 'codreceita': None, 'dhref': None,
                         'in_cancelamento_df': None, 'sq_cancelamento_df': None, 
                         'base_calculo' : None,
                         'ob': None
                         })
        
    df = pd.DataFrame(listaobs)
    return df



def get_doc_info(login, id, tokenhash):
    cookies = {
        'cpf': login,
        'hash': tokenhash,
    }
    params = {
        'chave': id,
    }
    response = requests.get('https://sag.eb.mil.br/php/chamadas/doc.php', params=params, cookies=cookies)
    if response.status_code != 200:
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find_all('tr')
    mapeamento = {}
    for row in table:
        if 'DADOS DA' in row.text:
            item_mapeamento = row.text.split('nº')[0].replace('\n', '').replace('\r', '').strip()
            mapeamento[item_mapeamento] = {}
            continue

        if 'DADOS CONTÁBEIS DA' in row.text:
            
            item_mapeamento = row.text.split('nº')[0].replace('\n', '').replace('\r', '').strip()
            mapeamento[item_mapeamento] = pd.DataFrame()
            continue
        if 'RELACIONADOS' in row.text:
            item_mapeamento = row.text.split('NR')[0].replace('\n', '').replace('\r', '').strip()
            mapeamento[item_mapeamento] = []
            continue

        if row.get('style') == None:
            if type(mapeamento[item_mapeamento]) == dict:
                try:
                    if 'ORDEM' in item_mapeamento:
                        mapeamento[item_mapeamento][row.find('td', {'colspan': '4'}).text] = row.find('td', {'colspan': '8'}).text
                    if 'DARF' in item_mapeamento:
                        mapeamento[item_mapeamento][row.find('td', {'colspan': '4'}).text] = row.find('td', {'colspan': '7'}).text
                    continue
                except:
                    pass
            if type(mapeamento[item_mapeamento]) == pd.DataFrame and 'DADOS CONTÁBEIS DA' in item_mapeamento:
                dicionario = {
                    'LINHA': row.find_all('td')[0].text,
                    'EVT': row.find_all('td')[1].text,
                    'CLAS1': row.find_all('td')[2].text,
                    'CLAS2': row.find_all('td')[3].text,
                    'INSC01': row.find_all('td')[4].text,
                    'INSC02': row.find_all('td')[5].text,
                    'ORC1': row.find_all('td')[6].text,
                    'ORC2': row.find_all('td')[7].text,
                    'UG_EMP': row.find_all('td')[8].text,
                    'VALOR': row.find_all('td')[9].text,
                    'MSG_CONF': row.find_all('td')[10].text,
                }
                mapeamento[item_mapeamento] = mapeamento[item_mapeamento]._append(dicionario, ignore_index=True)
                continue
            if type(mapeamento[item_mapeamento]) == list and 'RELACIONADOS' in item_mapeamento:
                mapeamento[item_mapeamento].append(row.find('a').get('id'))
    return mapeamento


def getUG(login, ug, tokenhash):
    cookies = {
        'cpf': login,
        'hash': tokenhash,
    }
    params = {
        'tipo': 'ug',
        'ug': ug,
    }
    response = requests.get('https://sag.eb.mil.br/php/chamadas/ug.php',
                             params=params, 
                             cookies=cookies, 
                             )
    if response.status_code == 200:
        return response.json()[0]
    

def validar_cnpj(cnpj):
    # Remove todos os caracteres não numéricos
    cnpj = ''.join(filter(str.isdigit, cnpj))
    
    # Verifica se o CNPJ tem exatamente 14 dígitos
    if len(cnpj) != 14:
        return None
    
    return cnpj

def limpatexto(texto):
    return bytes(texto.strip(), 'ISO-8859-1').decode('utf-8', 'ignore')

def processa_ob(login, item, tokenhash):
    i = item[0]
    row = item[1]
    id = row['id']
    doc = get_doc_info(login, id, tokenhash)
    if doc:
        return i, doc

    if doc == None:
        print(f'Erro ao processar {id}')
        return None



def main():
    print_welcome_banner()
    
    env_file = Path('.env')
    if not env_file.is_file():
        console.print("[yellow]Arquivo .env não encontrado. Criando novo arquivo...[/yellow]")
        env_file.touch()
    
    load_dotenv()
    
    login = os.getenv('LOGIN')
    senha = os.getenv('SENHA')
    
    if login is None and senha is None:
        login, senha = get_login_senha()
        
        # Salvando as credenciais no arquivo .env
        set_key(env_file, 'LOGIN', login)
        set_key(env_file, 'SENHA', senha)
        # set_key(env_file, 'SENHA', senha)
        console.print("[green]Credenciais salvas no arquivo .env[/green]")


    # Verifica se existe um tokenhash no arquivo .env
    tokenhash = os.getenv('hash')
    if tokenhash is None:
        # Verificar se o login e senha são válidos
        with console.status("[bold green]Verificando credenciais...[/bold green]"):
            console.print("[yellow]Verificando credenciais de acesso ao SAG...[/yellow]")
            tokenhash = make_login(login, senha)
            if tokenhash is None:
                console.print("[bold red]Erro ao verificar credenciais. Verifique o CPF e a senha e tente novamente.[/bold red]")
                os.remove(env_file)
                return
            console.print("[green]Credenciais válidas![/green]")
            set_key(env_file, 'hash', tokenhash)

    with console.status("[bold green]Iniciando processamento...[/bold green]"):
        console.print("[yellow]Obtendo dados do DARF...[/yellow]")
        df = getDARF(login, tokenhash)
        if df is None:
            console.print("[bold red]Erro ao obter dados do DARF. Verifique se o CPF informado tem acesso ao SAG e tente novamente.[/bold red]")
            os.remove(env_file)
            return
        console.print(f"[green]Dados obtidos com sucesso. Total de registros: {len(df)}[/green]")
    
    df = df[~df['desc'].str.contains('DARF NUMERADO AGREGADO')]
    # Remove DARFs com valor zerado
    df = df[df['valor'] != 0]
    # Remove DARFs com valor negativo
    df = df[df['valor'] > 0]

    df = df.reset_index(drop=True)
    
    treads = 20
    
    with Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        "•",
        TimeElapsedColumn(),
    ) as progress:
        task = progress.add_task("[cyan]Extraindo dados do SAG...", total=df.shape[0])
        
        try:
            with ThreadPoolExecutor(max_workers=treads) as executor:
                results = [executor.submit(processa_ob, login, item, tokenhash) for item in df.iterrows()]
                for f in as_completed(results):
                    progress.update(task, advance=1)
                freteList = [f.result() for f in results if f.result() is not None]
        except (KeyboardInterrupt, SystemExit):
            executor._threads.clear()
            concurrent.futures.thread._threads_queues.clear()
            console.print("[bold red]Processo interrompido pelo usuário.[/bold red]")
            return
    
    console.print("[green]Extraindo nome dos credores...[/green]")
    for item in freteList:
        i = item[0]
        doc = item[1]
        for key in doc.keys():
            if 'DADOS DA' in key:
                df.loc[i, 'dhref'] = doc[key]['GR_AN_NU_DOCUMENTO_REFERENCIA']
                df.loc[i, 'cpf/cnpj'] = doc[key]['IT_CO_FAVORECIDO']
                df.loc[i, 'codreceita'] = doc[key]['IT_CO_RECEITA']
                df.loc[i, 'in_cancelamento_df'] = doc[key]['IT_IN_CANCELAMENTO_DARF']
                df.loc[i, 'sq_cancelamento_df'] = doc[key]['IT_SQ_DF_CANCELAMENTO']
                df.loc[i, 'base_calculo'] = round(float(doc[key]['IT_VA_BASE_CALCULO'].replace('.', '').replace(',', '.')), 2)
            if 'RELACIONADOS' in key:
                for key2 in doc[key]:
                    if 'OB' in key2:
                        df.loc[i, 'ob'] = key2
    
    credores = df['cpf/cnpj'].unique()
    
    with Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
    ) as progress:
        task = progress.add_task("[cyan]Buscando credores...", total=len(credores))
        
        allcredores = {}
        for credor in credores:
            resultado = getCREDOR(login, credor, tokenhash)
            if resultado:
                allcredores[credor] = resultado
            else:
                pass
            progress.update(task, advance=1)
    
    for credor in allcredores.keys():
        df.loc[df['cpf/cnpj'] == credor, 'favorecido'] = allcredores[credor]['NOME']
    
    favorecidos_com_error = df.loc[df['favorecido'].isnull()]['favorecido']
    if favorecidos_com_error.shape[0] > 0:
        console.print(f"[yellow]Favorecidos com erro: {favorecidos_com_error.shape[0]}[/yellow]")
        console.print("[cyan]Tentando buscar os credores pela OB[/cyan]")
        
        with Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
        ) as progress:
            task = progress.add_task("[cyan]Buscando dados dos credores pela OB...", total=favorecidos_com_error.shape[0])
            
            for i, row in df.loc[df['favorecido'].isnull()].iterrows():
                if row['ob'] == None:
                    continue
                doc = get_doc_info(login, row['ob'], tokenhash)
                if doc:
                    for key in doc.keys():
                        if 'DADOS DA' in key:
                            novo_cnpj = doc[key]['IT_CO_FAVORECIDO']
                            if novo_cnpj != df.loc[i, 'cpf/cnpj']:
                                df.loc[i, 'cpf/cnpj'] = novo_cnpj
                                resultado = getCREDOR(login, df.loc[i, 'cpf/cnpj'], tokenhash)
                                if resultado:
                                    df.loc[i, 'favorecido'] = resultado['NOME']
                                else:
                                    continue
                progress.update(task, advance=1)
    favorecidos_com_error = df.loc[df['favorecido'].isnull()]['cpf/cnpj'].unique()
    if favorecidos_com_error.shape[0] > 0:
        console.print(f"[yellow]Favorecidos com erro:{len(favorecidos_com_error)}[/yellow]")
        console.print("[cyan]Favor verificar manualmente os favorecidos com erro[/cyan]")
        for favorecido in favorecidos_com_error:
            if favorecido == None or favorecido == '':
                continue
            if len(favorecido) == 6:
                console.print("[cyan]Foi encontrado uma UG no campo favorecido[/cyan]")
                novo_cnpj = console.input(f'[cyan]Informe o [green]CNPJ[/green] para a UG {favorecido}: [/cyan]')
                cnpj_valido = validar_cnpj(novo_cnpj)
                while cnpj_valido is None:
                    console.print("[bold red]CNPJ inválido. Deve conter exatamente 14 dígitos numéricos.[/bold red]")
                    novo_cnpj = console.input(f'[cyan]Informe o [green]CNPJ[/green] para a UG {favorecido}: [/cyan]')
                    cnpj_valido = validar_cnpj(novo_cnpj)
                df.loc[df['cpf/cnpj'] == favorecido, 'cpf/cnpj'] = novo_cnpj
                novo_favorecido = console.input(f"[cyan]Informe o [green]NOME[/green] do favorecido para o CNPJ {novo_cnpj}: [/cyan]")    
                df.loc[df['cpf/cnpj'] == novo_cnpj, 'favorecido'] = limpatexto(novo_favorecido.upper())
                continue
            novo_favorecido = console.input(f"[cyan]Informe o [green]NOME[/green] do favorecido para o CNPJ {favorecido}: [/cyan]")        
            df.loc[df['cpf/cnpj'] == favorecido, 'favorecido'] = limpatexto(novo_favorecido.upper())

    favorecidos_com_error = df.loc[df['favorecido'].isnull()]['favorecido'].unique()
    if favorecidos_com_error.shape[0] > 0:
        df.loc[df['favorecido'].isnull()].to_csv("favorecidos_com_erro.csv")
        console.print("[yellow]Favorecidos com erro salvos em favorecidos_com_erro_1ten_mello.csv[/yellow]")
    else:
        console.print("[green]Todos os favorecidos foram encontrados e os darfs foram processados com sucesso! [/green]")
    
    df = df.loc[df['favorecido'].notnull()]
    
    df['mes'] = pd.to_datetime(df['data'], format='mixed', dayfirst=True).dt.month

    ugs = {}
    for ug in df['ug'].unique():
        ugs[ug] = df.loc[df['ug'] == ug]
    
    console.print("[cyan]Gerando arquivos para DIRF, aguarde...[/cyan]")
    enviar_dados_para_mongodb(login, ugs)
    
    for ug in ugs.keys():
        dadosug = getUG(login, ug, tokenhash)
        if len(dadosug["CNPJ"]) < 14:
            dadosug["CNPJ"] = "0" + dadosug["CNPJ"]
        
        line1 = f'DIRF|2024|2023|N||B3VH8RQ|\n'
        line2 = f'RESPO|{dadosug["CPF_TES_T"]}|{dadosug["NOME_TES_T"]}|99|99999999|999999|99999999||\n'
        line3 = f'DECPJ|{dadosug["CNPJ"]}|{limpatexto(dadosug["NOME_UG"])}|1|{dadosug["CPF_OD_T"]}|N|N|N|N|N|N|N|N||\n'
        
        filename = f'{ug}_1Ten_Mello.txt'
        with open(filename, 'w') as f:
            f.writelines([line1, line2, line3])
        
        df = ugs[ug]
        soma = df.groupby(['codreceita', 'cpf/cnpj', 'mes', 'favorecido']).sum()
        codreceitas = soma.index.get_level_values(0).unique()
        
        for codreceita in codreceitas:
            if codreceita in ["1162", "7811"]:
                continue
            
            linha = f"IDREC|{codreceita}|\n"
            with open(filename, 'a') as f:
                f.write(linha)
            
            soma_codreceita = soma.loc[codreceita]
            cnpj = ''
            
            for item in soma_codreceita.iterrows():
                novo_cnpj = item[0][0]
                mes = item[0][1]
                favorecido = item[0][2]
                valor = "{:.2f}".format(item[1]['valor']).replace('.', '')
                base_calculo = "{:.2f}".format(item[1]['base_calculo']).replace('.', '')
                
                cnpj = str(cnpj)
                if novo_cnpj != cnpj:
                    if cnpj != '':
                        with open(filename, 'a') as f:
                            barras = 14 - linhabase.count('|')
                            f.writelines([linhafav, linhabase + "|"*barras+"\n", linharet+ "|"*barras+"\n"])
                    
                    cnpj = str(novo_cnpj)
                    linhafav = f"BPJDEC|{cnpj}|{favorecido}|\n"
                    linhabase = "RTRT" + "|" * mes
                    linhabase += f"{base_calculo}"
                    linharet = "RTIRF" + "|" * mes
                    linharet += f"{valor}"
                    continue
                
                if novo_cnpj == cnpj:
                    barras = mes - linhabase.count('|')
                    linhabase = linhabase + "|"*barras
                    linhabase = linhabase + f"{base_calculo}"
                    linharet = linharet + "|"*barras
                    linharet = linharet + f"{valor}"
            
            with open(filename, 'a') as f:
                barras = 14 - linhabase.count('|')
                f.writelines([linhafav, linhabase + "|"*barras+"\n", linharet+ "|"*barras+"\n"])
        
        with open(filename, 'a') as f:
            f.write('FIMDIRF|')
        
        console.print(f"[green]Arquivo gerado: {filename}[/green]")
    
    console.print("[bold green]Processamento concluído![/bold green]")
    total = 0
    for ug in ugs.keys():
        total += len(ugs[ug])

    total_favorecidos = 0
    for ug in ugs.keys():
        total_favorecidos += len(ugs[ug]['favorecido'].unique())
    table = Table(title="Resumo do Processamento")
    table.add_column("Dados", style="cyan")
    table.add_column("Valor", style="magenta", justify="center")
    table.add_row("Total de favorecidos", str(total_favorecidos))
    table.add_row("Total de DARF processadas", str(total))
    table.add_row("Total de Ugs processados", str(len(ugs)))
    table.add_row("Arquivos gerados", str(len(ugs)))
    console.print(table, justify='center')
    # Enviar dados para o MongoDB
    

if __name__ == "__main__":
    try:
        main()
        console.print("[green][bold]Projeto desenvolvido pelo 1º Ten Mello[/bold][/green]", justify='center')
        console.print("[yellow]Este programa te ajudou de alguma forma ? Pague um café para o desenvolvedor, PIX:[/yellow]", justify='center')
        console.print("[green][bold]mello.pedro@eb.mil.br[/bold][/green]", justify='center')
        console.input("Pressione ENTER para sair...")
    except Exception as e:
        console.print(traceback.format_exc())
        console.print(f"[bold red]Erro durante a execução:[/bold red]")
        console.print("[yellow]Entre em contato com o desenvolvedor para mais informações:[/yellow] [bold]1º Ten Mello - mello.pedro@eb.mil.br[/bold]")
        console.print("[yellow]")
        console.input("Pressione ENTER para sair...")