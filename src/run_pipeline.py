import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = Path(__file__).resolve().parent

def python_executable():
    """Interpreter used to run each pipeline step.

    Prefer the project virtualenv when it exists, using this platform's
    layout: ``venv/bin/python`` on macOS and Linux, and
    ``venv/Scripts/python.exe`` on Windows. The path is not resolved,
    because the Unix venv entry point is a symlink and the environment
    is applied only when that path is executed. If that interpreter is
    absent, use the interpreter that launched this process.
    """
    if sys.platform == "win32":
        candidate = PROJECT_ROOT / "venv" / "Scripts" / "python.exe"
    else:
        candidate = PROJECT_ROOT / "venv" / "bin" / "python"
    if candidate.is_file():
        return str(candidate)
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