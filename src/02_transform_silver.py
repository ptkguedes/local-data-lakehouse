import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def run_silver_transformation():
    print("🧹 [SILVER] Iniciando Processamento e Limpeza de Dados...")
    
    con = duckdb.connect()
    
    # 1. Localizar o arquivo JSON bruto mais recente na Bronze
    bronze_dir = PROJECT_ROOT / "data" / "bronze"
    bronze_files = sorted(bronze_dir.glob("flights_raw_*.json"))
    if not bronze_files:
        print("❌ Nenhum arquivo encontrado na Camada Bronze!")
        return
        
    latest_bronze_file = bronze_files[-1].as_posix()
    print(f"📖 Lendo dado bruto de: {latest_bronze_file}")
    
    # 2. Query de Transformação Silver (SEM ';' no final de WHERE row_num = 1)
    query_silver = f"""
        WITH raw_data AS (
            SELECT 
                flight_id,
                origin,
                destination,
                status,
                TRY_CAST(passengers AS INTEGER) as passengers,
                TRY_CAST(timestamp AS TIMESTAMP) as flight_timestamp,
                _ingested_at
            FROM read_json_auto('{latest_bronze_file}')
        ),
        deduplicated AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY flight_id, flight_timestamp 
                    ORDER BY _ingested_at DESC
                ) as row_num
            FROM raw_data
            WHERE destination IS NOT NULL
        )
        SELECT 
            flight_id,
            origin,
            destination,
            status,
            passengers,
            flight_timestamp
        FROM deduplicated
        WHERE row_num = 1
    """
    
    # 3. Persistir na Camada Silver
    silver_dir = PROJECT_ROOT / "data" / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)
    silver_output_path = (silver_dir / "flights_cleaned.parquet").as_posix()
    
    con.execute(f"""
        COPY ({query_silver}) 
        TO '{silver_output_path}' 
        (FORMAT PARQUET, COMPRESSION SNAPPY);
    """)
    
    print(f"💾 Tabela Silver salva em Parquet: {silver_output_path}")
    
    # 4. Validar resultado
    df_silver = con.execute(f"SELECT * FROM '{silver_output_path}'").df()
    print("\n--- Visualização da Camada Silver (Dado Limpo, Tipado e Deduplicado) ---")
    print(df_silver)
    
    con.close()
    print("✅ [SILVER] Transformação concluída com sucesso!")

if __name__ == "__main__":
    run_silver_transformation()