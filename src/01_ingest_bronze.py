import json
from datetime import datetime
from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def generate_raw_flight_data():
    """Simula o recebimento de payload bruto vindo de uma API de operações."""
    return [
        {"flight_id": "LA8001", "origin": "GIG", "destination": "GRU", "status": "SCHEDULED", "passengers": "180", "timestamp": "2026-10-07 10:00:00"},
        {"flight_id": "G31520", "origin": "GIG", "destination": "BSB", "status": "DELAYED", "passengers": "142", "timestamp": "2026-10-07 10:15:00"},
        {"flight_id": "AD4090", "origin": "SDU", "destination": "VCP", "status": "BOARDING", "passengers": "110", "timestamp": "2026-10-07 10:30:00"},
        # Registro propositalmente "sujo" para tratar na camada Silver depois:
        {"flight_id": "LA8001", "origin": "GIG", "destination": "GRU", "status": "SCHEDULED", "passengers": "180", "timestamp": "2026-10-07 10:00:00"}, 
        {"flight_id": "UNKNOWN", "origin": "GIG", "destination": None, "status": "CANCELLED", "passengers": "0", "timestamp": "2026-10-07 11:00:00"}
    ]

def run_bronze_ingestion():
    print("🚀 [BRONZE] Iniciando Ingestão de Dados Brutos...")
    
    # 1. Gerar dados simulados e gravar o horário de ingestão no próprio arquivo
    raw_data = generate_raw_flight_data()
    ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for record in raw_data:
        record["_ingested_at"] = ingested_at
    
    # 2. Salvar o JSON bruto na camada Bronze (Auditoria e Imutabilidade)
    bronze_dir = PROJECT_ROOT / "data" / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_filepath = bronze_dir / f"flights_raw_{today_str}.json"
    json_sql_path = json_filepath.as_posix()
    
    with open(json_filepath, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2)
    print(f"📦 Archive JSON bruto salvo em: {json_filepath}")
    
    # 3. Registrar na tabela Bronze usando DuckDB
    con = duckdb.connect()
    
    query = f"""
        CREATE TABLE IF NOT EXISTS flights_bronze AS 
        SELECT * 
        FROM read_json_auto('{json_sql_path}');
    """
    con.execute(query)
    
    # Exibir preview da camada Bronze, incluindo _ingested_at persistido no JSON
    df_bronze = con.execute("SELECT * FROM flights_bronze").df()
    print("\n--- Visualização da Camada Bronze (Dado Bruto + Metadata) ---")
    print(df_bronze)
    
    con.close()
    print("✅ [BRONZE] Ingestão concluída com sucesso!")

if __name__ == "__main__":
    run_bronze_ingestion()