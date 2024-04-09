import requests
import datetime
import logging
import boto3
import os
import certifi
from urllib3.exceptions import InsecureRequestWarning
 
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(message)s')
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

ano = datetime.datetime.now().strftime('%Y')

# URL do arquivo XLS que você deseja baixar
url = f'https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/IMP_{ano}_MUN.csv'
cert = certifi.where()
# Realiza a requisição GET para baixar o arquivo
response = requests.get(url, verify=False)

# Verifica se a requisição foi bem-sucedida (código 200)
if response.status_code == 200:
    # Obtém a data e hora atual
    data_hora_atual = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Extrai o nome do arquivo da URL
    nome_arquivo = url.split('/')[-1]
    
    # Renomeia o arquivo baixado com o padrão especificado
    novo_nome_arquivo = f"MUN_{nome_arquivo.split('.')[0]}-{data_hora_atual}.{nome_arquivo.split('.')[-1]}"
    
    # Salva o conteúdo do arquivo baixado com o novo nome
    with open(f'./pyscripts/temp/{novo_nome_arquivo}', 'wb') as f:
        f.write(response.content)
    
    # Log da mensagem de sucesso
    logging.info(f"Arquivo baixado com sucesso como '{novo_nome_arquivo}'.")
    
    try:
        # Envia o arquivo para o bucket S3
        s3 = boto3.client('s3')
        bucket_name = 'geocomex42'
        object_name = f"bronze/imp/brasil/aberto/{novo_nome_arquivo}"  # Nome do objeto no S3 (pode ser uma pasta)
        
        # Faz o upload do arquivo para o S3
        s3.upload_file(f'./pyscripts/temp/{novo_nome_arquivo}', bucket_name, object_name)
    except:
        logging.info(f'Erro no envio do arquivo {object_name} para o bucket S3.')    
    else:
        # Log da mensagem de sucesso no upload
        logging.info(f"Arquivo enviado para o bucket S3 como '{object_name}'.")
    finally:
        os.remove(f'./pyscripts/temp/{novo_nome_arquivo}')
else:
    # Log da mensagem de falha
    logging.error("Falha ao baixar o arquivo.")