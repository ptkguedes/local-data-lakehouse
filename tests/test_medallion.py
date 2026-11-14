import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_script(filename, module_name):
    path = ROOT / "src" / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def fetch_all(path, columns):
    con = duckdb.connect()
    try:
        query = "SELECT " + ", ".join(columns) + f" FROM '{path.as_posix()}'"
        return con.execute(query).fetchall()
    finally:
        con.close()


def test_silver_cleans_latest_bronze_file(tmp_path):
    bronze = tmp_path / "data" / "bronze"
    write_json(
        bronze / "flights_raw_20200101_000000.json",
        [
            {
                "flight_id": "OLD1",
                "origin": "GRU",
                "destination": "SSA",
                "status": "SCHEDULED",
                "passengers": "10",
                "timestamp": "2020-01-01 00:00:00",
                "_ingested_at": "2020-01-01 00:00:00",
            }
        ],
    )
    write_json(
        bronze / "flights_raw_20261007_120000.json",
        [
            {
                "flight_id": "LA8001",
                "origin": "GIG",
                "destination": "GRU",
                "status": "SCHEDULED",
                "passengers": "180",
                "timestamp": "2026-10-07 10:00:00",
                "_ingested_at": "2026-10-07 09:00:00",
            },
            {
                "flight_id": "LA8001",
                "origin": "GIG",
                "destination": "GRU",
                "status": "SCHEDULED",
                "passengers": "200",
                "timestamp": "2026-10-07 10:00:00",
                "_ingested_at": "2026-10-07 12:00:00",
            },
            {
                "flight_id": "G31520",
                "origin": "GIG",
                "destination": "BSB",
                "status": "DELAYED",
                "passengers": "142",
                "timestamp": "2026-10-07 10:15:00",
                "_ingested_at": "2026-10-07 09:00:00",
            },
            {
                "flight_id": "CX100",
                "origin": "GIG",
                "destination": "GRU",
                "status": "CANCELLED",
                "passengers": "0",
                "timestamp": "2026-10-07 12:00:00",
                "_ingested_at": "2026-10-07 09:00:00",
            },
            {
                "flight_id": "UNKNOWN",
                "origin": "GIG",
                "destination": None,
                "status": "CANCELLED",
                "passengers": "0",
                "timestamp": "2026-10-07 11:00:00",
                "_ingested_at": "2026-10-07 09:00:00",
            },
        ],
    )

    silver = load_script("02_transform_silver.py", "transform_silver")
    silver.run_silver_transformation(tmp_path)

    output = tmp_path / "data" / "silver" / "flights_cleaned.parquet"
    rows = fetch_all(
        output,
        ["flight_id", "origin", "destination", "status", "passengers", "flight_timestamp"],
    )
    by_flight = {row[0]: row for row in rows}

    assert set(by_flight) == {"LA8001", "G31520", "CX100"}
    assert by_flight["LA8001"] == (
        "LA8001",
        "GIG",
        "GRU",
        "SCHEDULED",
        200,
        datetime(2026, 10, 7, 10, 0, 0),
    )
    assert by_flight["G31520"][4] == 142
    assert by_flight["CX100"][3:] == ("CANCELLED", 0, datetime(2026, 10, 7, 12, 0, 0))


def test_silver_skips_when_bronze_is_missing(tmp_path):
    silver = load_script("02_transform_silver.py", "transform_silver")
    silver.run_silver_transformation(tmp_path)
    assert not (tmp_path / "data" / "silver" / "flights_cleaned.parquet").exists()


def test_gold_aggregates_by_origin_and_status(tmp_path):
    silver_path = tmp_path / "data" / "silver" / "flights_cleaned.parquet"
    silver_path.parent.mkdir(parents=True)
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE flights (
            flight_id VARCHAR,
            origin VARCHAR,
            destination VARCHAR,
            status VARCHAR,
            passengers INTEGER,
            flight_timestamp TIMESTAMP
        )
        """
    )
    con.executemany(
        "INSERT INTO flights VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("A1", "GIG", "GRU", "SCHEDULED", 100, datetime(2026, 10, 7, 10, 0, 0)),
            ("A2", "GIG", "SSA", "SCHEDULED", 101, datetime(2026, 10, 7, 11, 0, 0)),
            ("B1", "GIG", "BSB", "DELAYED", 40, datetime(2026, 10, 7, 10, 15, 0)),
            ("C1", "SDU", "VCP", "BOARDING", 50, datetime(2026, 10, 7, 10, 30, 0)),
        ],
    )
    con.execute(
        f"""
        COPY flights TO '{silver_path.as_posix()}'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
        """
    )
    con.close()

    gold = load_script("03_aggregate_gold.py", "aggregate_gold")
    gold.run_gold_aggregation(tmp_path)

    output = tmp_path / "data" / "gold" / "kpi_flights_summary.parquet"
    rows = fetch_all(
        output,
        [
            "origin",
            "status",
            "total_flights",
            "total_passengers",
            "avg_passengers_per_flight",
            "(_calculated_at IS NOT NULL) AS has_calculated_at",
        ],
    )

    assert rows[0][0:2] == ("GIG", "SCHEDULED")
    by_group = {}
    for origin, status, flights, passengers, average, has_calculated_at in rows:
        assert has_calculated_at is True
        by_group[(origin, status)] = (int(flights), int(passengers), float(average))

    assert by_group[("GIG", "SCHEDULED")] == (2, 201, pytest.approx(100.5))
    assert by_group[("GIG", "DELAYED")] == (1, 40, pytest.approx(40.0))
    assert by_group[("SDU", "BOARDING")] == (1, 50, pytest.approx(50.0))


def test_gold_skips_when_silver_is_missing(tmp_path):
    gold = load_script("03_aggregate_gold.py", "aggregate_gold")
    gold.run_gold_aggregation(tmp_path)
    assert not (tmp_path / "data" / "gold" / "kpi_flights_summary.parquet").exists()


@pytest.mark.parametrize(
    ("platform", "relative"),
    [
        ("linux", Path("venv") / "bin" / "python"),
        ("darwin", Path("venv") / "bin" / "python"),
        ("win32", Path("venv") / "Scripts" / "python.exe"),
    ],
)
def test_python_executable_prefers_platform_venv(tmp_path, monkeypatch, platform, relative):
    runner = load_script("run_pipeline.py", "run_pipeline")
    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(runner.sys, "platform", platform)
    candidate = tmp_path / relative
    candidate.parent.mkdir(parents=True)
    candidate.symlink_to(Path(sys.executable))

    assert runner.python_executable() == str(candidate)


@pytest.mark.parametrize(
    ("platform", "ignored"),
    [
        ("linux", Path("venv") / "Scripts" / "python.exe"),
        ("darwin", Path("venv") / "Scripts" / "python.exe"),
        ("win32", Path("venv") / "bin" / "python"),
    ],
)
def test_python_executable_ignores_other_platform_layout(tmp_path, monkeypatch, platform, ignored):
    runner = load_script("run_pipeline.py", "run_pipeline")
    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(runner.sys, "platform", platform)
    decoy = tmp_path / ignored
    decoy.parent.mkdir(parents=True)
    decoy.write_bytes(b"")

    assert runner.python_executable() == sys.executable


def test_python_executable_defaults_to_current_interpreter(tmp_path, monkeypatch):
    runner = load_script("run_pipeline.py", "run_pipeline")
    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)

    assert runner.python_executable() == sys.executable
