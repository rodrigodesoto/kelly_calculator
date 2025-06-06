import yfinance as yf
import pandas as pd
from pymongo import MongoClient
from bson import ObjectId
import time
from tqdm import tqdm
from dotenv import load_dotenv
import os

# Carregar as variáveis do .env
load_dotenv()

# Recuperar as variáveis
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

# Conexão com MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# Recupera todos os documentos
all_docs = list(collection.find())
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
        hist = ticker.history(period="1y")  # últimos 12 meses

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

        print(f"Kelly fraction para {symbol}: {kelly_fraction:.4f}")

        # Estrutura para salvar no MongoDB
        kelly_data = {
            "_id": ObjectId(),
            "data": pd.Timestamp.now().isoformat(),
            "%_kelly": round(kelly_fraction, 6)
        }
        # Se já existe o campo kelly_fraction, adiciona, senão cria
        if 'kelly_fraction' in doc and isinstance(doc['kelly_fraction'], list):
            updated_kelly = doc['kelly_fraction']
            updated_kelly.append(kelly_data)
        else:
            updated_kelly = [kelly_data]

        # Atualizar o documento no MongoDB
        # collection.update_one(
        #     {"_id": doc["_id"]},
        #     {"$set": {"kelly_fraction": updated_kelly}}
        # )

        print(f"Kelly fraction salvo no MongoDB para {symbol}")


    except Exception as e:
        print(f"\n❌ Erro ao processar {symbol}: {e}")

print("Processamento concluído!")


# Calcula tempo total
end_time = time.time()
elapsed_time = end_time - start_time  # segundos

minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print(f"\n✅ Tempo total:  {minutes:02d}:{seconds:02d} (mm:ss) para processar {total} tickers.")