#fetch_nfse.py

import os
import requests
import base64
import re
import json
import logging
from datetime import datetime


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

with open(CONFIG_PATH, "r") as f:
    CONFIG = json.load(f)


API_URL = CONFIG["api_url"]
HEADERS = {
    "X-API-ID": CONFIG["api_id"],
    "X-API-KEY": CONFIG["api_key"],
    "Content-Type": "application/json"
}

LOG_DIR = os.path.join(BASE_DIR, CONFIG["log_directory"])
RESPONSE_DIR = os.path.join(BASE_DIR, CONFIG["response_directory"])


if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

if not os.path.exists(RESPONSE_DIR):
    os.makedirs(RESPONSE_DIR)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "nfse.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def save_api_response(data):
    """Salva resposta da API para auditoria."""
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(RESPONSE_DIR, f"api_response_{now}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def save_api_response(data):
    """Salva resposta da API para auditoria."""
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(RESPONSE_DIR, f"api_response_{now}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def save_xml(document):
    """Salva XML decodificado."""
    encoded_xml = document.get("xml")
    doc_id = document.get("id")

    if not encoded_xml or not doc_id:
        logging.warning("XML ou ID ausente no documento.")
        return

    decoded_xml = base64.b64decode(encoded_xml).decode("utf-8")
    cnpj = extract_cnpj_from_xml(decoded_xml)  
    directory = get_directory_for_cnpj(cnpj)

    if not directory:
        logging.warning(f"CNPJ {cnpj} não pertence a nenhuma categoria.")
        return

    processed_directory = CONFIG["directories"]["processados_hering"] if cnpj in CONFIG["cnpjs"]["hering"] else CONFIG["directories"]["processados_guess"]

    existing_files = [
        os.path.join(processed_directory, f"{doc_id}.xml"),
        os.path.join(directory, f"{doc_id}.xml")
    ]

    if all(os.path.exists(file) for file in existing_files):
        return doc_id  # Retorna o ID do documento já processado

    if not os.path.exists(directory):
        logging.warning(f"O diretório {directory} não existe. Não foi possível salvar o arquivo.")
        return  

    file_path = os.path.join(directory, f"{doc_id}.xml")

    with open(file_path, "w", encoding="utf-8") as file:
        file.write(decoded_xml)
    
    return doc_id  # Retorna o ID do documento processado



def extract_cnpj_from_xml(xml_content):
    """Extrai CNPJ do XML."""
    match = re.search(r"<Tomador>.*?<Cnpj>(\d+)</Cnpj>.*?</Tomador>", xml_content)
    return match.group(1) if match else ""

def get_directory_for_cnpj(cnpj):
    """Decide o diretório com base no CNPJ."""
    if cnpj in CONFIG["cnpjs"]["hering"]:
        return CONFIG["directories"]["hering"]
    if cnpj in CONFIG["cnpjs"]["guess"]:
        return CONFIG["directories"]["guess"]
    return None

def extract_cursor_from_url(url):
    """Extrai cursor de URL."""
    match = re.search(r"cursor=(\d+)", url)
    return int(match.group(1)) if match else 0

def process_documents(documents, total_processed, total_already_processed, total_skipped):
    """Processa os documentos e gera logs de forma agrupada no final."""
    
    for document in documents:
        doc_id = document.get("id")
        encoded_xml = document.get("xml")

        if not encoded_xml or not doc_id:
            total_skipped.add(doc_id)
            continue

        decoded_xml = base64.b64decode(encoded_xml).decode("utf-8")
        cnpj = extract_cnpj_from_xml(decoded_xml)

        empresa = "HERING" if cnpj in CONFIG["cnpjs"]["hering"] else "GUESS" if cnpj in CONFIG["cnpjs"]["guess"] else None

        if not empresa:
            total_skipped.add(doc_id)
            continue  

        processed_directory = CONFIG["directories"][f"processados_{empresa.lower()}"]  
        processed_file_path = os.path.join(processed_directory, f"{doc_id}.xml")

        if os.path.exists(processed_file_path):
            total_already_processed[empresa].add(doc_id)
            continue  

        if save_xml(document):  
            total_processed[empresa].add(doc_id)

def fetch_nfse():
    """Busca e processa NFS-e de todas as páginas."""
    now = datetime.now()
    start_date = now.strftime("%Y-%m-01")
    end_date = now.strftime("%Y-%m-%d")

    params = {
        "created_at[from]": f"{start_date} 00:00:00",
        "created_at[to]": f"{end_date} 23:59:59",
        "limit": 50,
    }
    
    cursor = 0
    has_next_page = True
    previous_cursor = None  

    # Acumuladores para evitar logs repetidos
    total_processed = {"HERING": set(), "GUESS": set()}
    total_already_processed = {"HERING": set(), "GUESS": set()}
    total_skipped = set()

    while has_next_page:
        if cursor == previous_cursor: 
            logging.warning("Cursor não avançou. Interrompendo para evitar loop infinito.")
            break

        previous_cursor = cursor 
        params["cursor"] = cursor

        try:
            response = requests.get(API_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("data"):  
                save_api_response(data)
                process_documents(data.get("data"), total_processed, total_already_processed, total_skipped)

            next_cursor = data.get("page", {}).get("next")
            cursor = extract_cursor_from_url(next_cursor) if next_cursor else 0
            has_next_page = bool(next_cursor)

        except Exception as e:
            logging.error(f"Erro ao buscar dados: {e}")
            has_next_page = False

    # Logs organizados (apenas uma vez no final)
    for empresa, docs in total_processed.items():
        if docs:
            logging.info(f"{len(docs)} arquivos processados para {empresa}.")
    
    for empresa, docs in total_already_processed.items():
        if docs:
            logging.info(f"{len(docs)} arquivos já estavam processados para {empresa}.")
    
    if not any(total_processed.values()):
        logging.info("Todos os arquivos já estavam processados anteriormente.")

    if total_skipped:
        logging.warning(f"{len(total_skipped)} arquivos ignorados (inválidos ou sem categoria).")

if __name__ == "__main__":
    fetch_nfse()
