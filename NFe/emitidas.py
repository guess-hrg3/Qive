import requests
import base64
import os

# Credenciais da API Arquivei
API_ID = "2240670422fadb214d05eb5d23dda171ddb297b8"
API_KEY = "b79afb3ae5eb31729c8277dcfacc09032ffc215e"
API_URL = "https://api.arquivei.com.br/v1/nfse/emitted"

def baixar_xml_nfse(chave_nfse, pasta_destino="xmls"):
    """
    Busca uma NFSe emitida na Arquivei pela chave e salva o XML decodificado.
    """
    headers = {
        "Content-Type": "application/json",
        "x-api-id": API_ID,
        "x-api-key": API_KEY,
    }

    params = {
        "id[]": chave_nfse,
        "format_type": "xml",
        "limit": 1
    }

    print(f"Buscando NFSe {chave_nfse}...")

    response = requests.get(API_URL, headers=headers, params=params)
    if response.status_code != 200:
        print(f"Erro {response.status_code}: {response.text}")
        return

    data = response.json()

    # Normalmente o XML vem em base64 no campo 'xml' ou 'data[i].xml'
    documentos = data.get("data", [])
    if not documentos:
        print("Nenhum documento encontrado para a chave fornecida.")
        return

    xml_b64 = documentos[0].get("xml") or documentos[0].get("content")
    if not xml_b64:
        print("Campo 'xml' não encontrado na resposta.")
        return

    xml_decodificado = base64.b64decode(xml_b64).decode("utf-8")

    os.makedirs(pasta_destino, exist_ok=True)
    caminho_arquivo = os.path.join(pasta_destino, f"{chave_nfse}.xml")

    with open(caminho_arquivo, "w", encoding="utf-8") as f:
        f.write(xml_decodificado)

    print(f"✅ XML salvo em: {caminho_arquivo}")

if __name__ == "__main__":
    chave = "35251017809524000392550040000914521082219540"
    baixar_xml_nfse(chave)
