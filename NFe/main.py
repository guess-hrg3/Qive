import os
import json
import requests
import base64
from datetime import datetime, timedelta
from NFe.utils.logger import configurar_logger
from urllib.parse import urlparse, parse_qs

logger = configurar_logger()


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)


API_URL = config["api_url"]
API_ID = config["api_id"]
API_KEY = config["api_key"]
HEADERS = {
    "Content-Type": "application/json",
    "x-api-id": API_ID,
    "x-api-key": API_KEY
}

def carregar_grupos():
    return config["cnpjs"]

def extrair_cursor(url):
    if not url:
        return None
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    cursor_list = query_params.get('cursor')
    if cursor_list:
        return cursor_list[0]
    return None

def obter_caminho_destino(nome_grupo):
    dirs = config.get("directories", {})
    base_path = ""
    if nome_grupo == "guess":
        base_path = dirs.get("guess")
    elif nome_grupo == "hering":
        base_path = dirs.get("hering")
    else:
        base_path = os.path.join("nfes", nome_grupo)

    processar = os.path.join(base_path, "Processar")
    processados = os.path.join(base_path, "Processado")
    return processar, processados  # retorna os dois caminhos

def baixar_xmls(cnpjs, nome_grupo):
    hoje = datetime.now()
    sete_dias = hoje - timedelta(days=7)

    params = {
        "cnpj[]": cnpjs,
        "format_type": "xml",
        "created_at[from]": sete_dias.strftime("%Y-%m-%d") + " 00:00:00",
        "created_at[to]": hoje.strftime("%Y-%m-%d") + " 23:59:59",
        "limit": 50,
    }

    pasta_processar, pasta_processado = obter_caminho_destino(nome_grupo)

    if not os.path.exists(pasta_processar):
        logger.error(f"❌ Pasta de destino não encontrada: {pasta_processar}")
        return
    if not os.path.exists(pasta_processado):
        logger.error(f"❌ Pasta de destino não encontrada: {pasta_processado}")

    cursor = None

    while True:
        if cursor and cursor != "0":
            params["cursor"] = cursor
        else:
            params.pop("cursor", None)

        try:
            response = requests.get(API_URL, headers=HEADERS, params=params, timeout=15)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na requisição: {e}")
            break

        try:
            data = response.json()
        except Exception as e:
            logger.error(f"Erro ao decodificar JSON: {e}")
            break

        notas = data.get("data", [])

        for nota in notas:
            chave = nota["access_key"]
            xml_base64 = nota["xml"]

            caminho_processar = os.path.join(pasta_processar, f"{chave}.xml")
            caminho_processado = os.path.join(pasta_processado, f"{chave}.xml")

            if os.path.exists(caminho_processar) or os.path.exists(caminho_processado):
                # Se já existe em qualquer uma das pastas, pula
                continue

            try:
                xml_decodificado = base64.b64decode(xml_base64).decode('utf-8')
            except Exception as e:
                logger.error(f"Erro ao decodificar XML base64 da chave {chave}: {e}")
                continue

            try:
                with open(caminho_processar, "w", encoding="utf-8") as f:
                    f.write(xml_decodificado)
                logger.info(f"XML salvo: {caminho_processar}")
            except Exception as e:
                logger.error(f"❌ Erro ao salvar arquivo {caminho_processar}: {e}")

        next_page_url = data.get("page", {}).get("next")
        proximo_cursor = extrair_cursor(next_page_url)

        if proximo_cursor == cursor and len(notas) == 0:
            logger.info(f"sem novas notas, finalizando paginação.")
            break

        if not proximo_cursor or proximo_cursor == "0":
            logger.info(f"Fim da paginação para o grupo {nome_grupo}.")
            break

        cursor = proximo_cursor


def executar_coleta():
    logger.info("Iniciando coleta de XMLs...")
    grupos = carregar_grupos()
    for nome_grupo, cnpjs in grupos.items():
        logger.info(f"Iniciando grupo: {nome_grupo}")
        baixar_xmls(cnpjs, nome_grupo)
    logger.info("Coleta finalizada com sucesso.")

if __name__ == "__main__":
    executar_coleta()
