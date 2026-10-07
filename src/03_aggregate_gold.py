import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def run_gold_aggregation():
    print("🏆 [GOLD] Iniciando Agregação de Negócio e Geração de KPIs...")
    
    con = duckdb.connect()
    silver_path = PROJECT_ROOT / "data" / "silver" / "flights_cleaned.parquet"
    
    if not silver_path.exists():
        print("❌ Arquivo da Camada Silver não encontrado! Execute a Silver primeiro.")
        return

    silver_sql_path = silver_path.as_posix()
        
    # 1. Agregação de Negócio (KPIs por Origem e Status)
    query_gold = f"""
        SELECT 
            origin,
            status,
            COUNT(flight_id) as total_flights,
            SUM(passengers) as total_passengers,
            ROUND(AVG(passengers), 1) as avg_passengers_per_flight,
            CURRENT_TIMESTAMP as _calculated_at
        FROM '{silver_sql_path}'
        GROUP BY origin, status
        ORDER BY total_flights DESC
    """
    
    # 2. Persistir na Camada Gold em formato Parquet de alta performance (evita DLLs externas bloqueadas)
    gold_dir = PROJECT_ROOT / "data" / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    gold_output_path = (gold_dir / "kpi_flights_summary.parquet").as_posix()
    
    con.execute(f"""
        COPY ({query_gold}) 
        TO '{gold_output_path}' 
        (FORMAT PARQUET, COMPRESSION SNAPPY);
    """)
    
    print(f"🥇 Tabela Gold de KPIs salva em: {gold_output_path}")
    
    # 3. Validar resultado lendo a tabela Gold
    print("\n--- Visualização da Camada Gold (KPIs de Negócio Consolidados) ---")
    df_read_gold = con.execute(f"SELECT * FROM '{gold_output_path}'").df()
    print(df_read_gold)
    
    con.close()
    print("✅ [GOLD] Arquitetura Medallion concluída com sucesso!")

if __name__ == "__main__":
    run_gold_aggregation()