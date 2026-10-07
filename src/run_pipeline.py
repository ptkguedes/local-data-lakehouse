import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = Path(__file__).resolve().parent

def python_executable():
    venv_python = PROJECT_ROOT / "venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable

def run_step(script_name):
    print(f"\n==========================================")
    print(f"🔄 Executando: {script_name}")
    print(f"==========================================")

    script_path = SRC_DIR / script_name
    result = subprocess.run([python_executable(), str(script_path)], cwd=PROJECT_ROOT)
    
    if result.returncode != 0:
        print(f"❌ Erro ao executar {script_name}. Pipeline interrompido.")
        sys.exit(1)

def main():
    start_time = time.time()
    print("🚀 INICIANDO ORQUESTRAÇÃO DO DATA LAKEHOUSE LOCAL")
    
    run_step("01_ingest_bronze.py")
    run_step("02_transform_silver.py")
    run_step("03_aggregate_gold.py")
    
    elapsed_time = round(time.time() - start_time, 2)
    print(f"\n✨ PIPELINE MEDALLION EXECUTADO COM SUCESSO EM {elapsed_time}s! ✨")

if __name__ == "__main__":
    main()