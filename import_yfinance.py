import yfinance as yf
import pandas as pd
from pymongo import MongoClient
from bson import ObjectId
import time
from tqdm import tqdm
from dotenv import load_dotenv
import os
from datetime import datetime
import sys

# Obter o dia da semana atual (0 = segunda, 6 = domingo)
hoje = datetime.today().weekday()

# Se for sábado (5) ou domingo (6), sair do programa
if hoje >= 5:
    print("Hoje não é dia útil. Encerrando a execução.")
    sys.exit()

# Carregar as variáveis do .env
load_dotenv()

# Recuperar as variáveis
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
COLLECTION_CFG = os.getenv("COLLECTION_CFG")
COLLECTION_ALL_STOCKS = os.getenv("COLLECTION_ALL_STOCKS")
COLLECTION_KELLY_FRACTION = os.getenv("COLLECTION_KELLY_FRACTION")


# Conexão com MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection_cfg = db[COLLECTION_CFG]
collection_all_stocks = db[COLLECTION_ALL_STOCKS]
collection_kelly_fraction = db[COLLECTION_KELLY_FRACTION]

# Buscar o período estatístico da collection cfg
cfg_doc = collection_cfg.find_one({"statisticalPeriod": {"$exists": True}})
if not cfg_doc:
    raise ValueError("Não foi encontrado um documento com o campo 'statisticalPeriod' na collection cfg.")
period = cfg_doc["statisticalPeriod"]
print(f"Período de análise configurado: {period}")

# Recupera todos os documentos
all_docs = list(collection_all_stocks.find())
total = len(all_docs)

# Inicia o cronômetro
start_time = time.time()

# Iterar sobre todos os documentos da collection
for doc in tqdm(all_docs, desc="Processando tickers", unit="ticker"):
    symbol = doc['symbol']
    yahoo_ticker = doc['yahoo_ticker']
    
    print(f"Processando {symbol}...")

    try:
        ticker = yf.Ticker(yahoo_ticker)
        hist = ticker.history(period=period)  # últimos 12 meses

        if hist.empty:
            print(f"Nenhum dado para {symbol}. Pulando...")
            continue

        # Salvar histórico em CSV
        # file_name = f'historico_{symbol}.csv'
        # hist.to_csv(file_name)
        # print(f"Histórico salvo em {file_name}")

        # Calcular b
        returns = hist['Close'].pct_change().dropna()
        if len(returns) == 0:
            continue  # pula se não tiver dados suficientes

        b = returns.mean()

        # Calcular p e q
        up_days = (returns > 0).sum()
        total_days = returns.count()

        p = up_days / total_days
        q = 1 - p

        # Calcular o Kelly Fraction
        if b != 0:
            kelly_fraction = (b * p - q) / b
        else:
            kelly_fraction = 0

        # Calcular Kelly Contínuo
        variance = returns.var()
        kelly_continuo = b / variance if variance != 0 else 0

        print(f"Kelly fraction para {symbol}: {kelly_fraction:.4f}")
        print(f"Kelly continuo para {symbol}: {kelly_continuo:.4f}")

        # Criar e salvar o documento na collection kelly_fraction
        kelly_doc = {
            "_id": ObjectId(),
            "symbol": symbol,
            "data": datetime.now(),
            "total_dias": returns.size,
            "retorno_medio_b": round(b, 6),
            "%_dias_positivos_p": round(p, 6),
            "%_dias_negativos_p": round(q, 6),
            "%_kelly": round(kelly_fraction, 6),
            "kelly_continuo": round(kelly_continuo, 6)
        }

        # Atualizar o documento no MongoDB
        collection_kelly_fraction.insert_one(kelly_doc)
        print(f"Salvo na collection kelly_fraction: {symbol}")

        # print(f"Kelly fraction salvo no MongoDB para {symbol}")


    except Exception as e:
        print(f"Erro ao processar {symbol}: {e}")

print("Processamento concluído!")


# Calcula tempo total
end_time = time.time()
elapsed_time = end_time - start_time  # segundos

minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print(f"Tempo total:  {minutes:02d}:{seconds:02d} (mm:ss) para processar {total} tickers.")