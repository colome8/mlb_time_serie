#!/usr/bin/env python3
"""
Descarga transacciones MLB (sportId=1) desde la API oficial y construye:
1) CSV de transacciones MLB (raw/flat)
2) CSV de eventos relacionados a lesión
3) CSV de conteo diario de "nuevas lesiones registradas" (colocaciones en IL)

Rango por defecto: 2015-01-01 a 2025-12-31
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.request import urlopen
from urllib.parse import urlencode


BASE_URL = "https://statsapi.mlb.com/api/v1/transactions"


INJURY_KEYWORDS = [
    "injured list",
    "disabled list",
    "concussion injured list",
    "covid-19 injured list",
    "rehab assignment",
]


IL_PLACEMENT_RE = re.compile(
    r"\bplaced\b.*?\bon the\b.*?\b(?:injured|disabled) list\b", re.IGNORECASE
)
IL_TRANSFER_RE = re.compile(
    r"\btransferred\b.*?\b(?:injured|disabled) list\b.*?\bto the\b.*?\b(?:injured|disabled) list\b",
    re.IGNORECASE,
)
IL_ACTIVATION_RE = re.compile(
    r"\b(activated|reinstated)\b.*?\bfrom the\b.*?\b(?:injured|disabled) list\b",
    re.IGNORECASE,
)
REHAB_ASSIGNMENT_RE = re.compile(r"\brehab assignment\b", re.IGNORECASE)
IL_DAY_CLASS_RE = re.compile(r"\b(7|10|15|60)-day (?:injured|disabled) list\b", re.IGNORECASE)


RAW_COLUMNS = [
    "transaction_id",
    "api_date",
    "effective_date",
    "resolution_date",
    "event_date",
    "year",
    "type_code",
    "type_desc",
    "description",
    "person_id",
    "person_name",
    "from_team_id",
    "from_team_name",
    "to_team_id",
    "to_team_name",
    "is_injury_related",
    "injury_event_type",
    "is_il_placement",
    "is_il_activation",
    "is_il_transfer",
    "is_rehab_assignment",
    "is_covid_il",
    "il_days_bucket",
]


INJURY_COLUMNS = RAW_COLUMNS + [
    "count_as_new_injury_registration",
]


DAILY_COLUMNS = [
    "date",
    "year",
    "injury_registrations",        # colocaciones nuevas en IL
    "injury_related_transactions", # cualquier evento de lesión (IL/rehab)
    "il_activations",
    "il_transfers",
    "rehab_assignments",
]


def fetch_json(url: str, retries: int = 3, sleep_seconds: float = 1.5) -> dict:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            with urlopen(url, timeout=120) as resp:
                return json.load(resp)
        except Exception as exc:  # pragma: no cover - runtime/network guard
            last_error = exc
            if attempt == retries:
                break
            time.sleep(sleep_seconds * attempt)
    raise RuntimeError(f"Error consultando API: {url}") from last_error


def build_url(start_date: str, end_date: str, sport_id: int = 1) -> str:
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "sportId": sport_id,
    }
    return f"{BASE_URL}?{urlencode(params)}"


def safe_get(obj: Optional[dict], key: str) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    return obj.get(key)


def choose_event_date(tx: dict) -> Optional[str]:
    # Para "registradas por día" usamos la fecha del registro en API.
    # effectiveDate se conserva aparte, pero puede ser retroactiva o venir con datos raros.
    return tx.get("date") or tx.get("effectiveDate") or tx.get("resolutionDate")


def classify_injury(description: str) -> Dict[str, object]:
    text = (description or "").strip()
    lower = text.lower()

    is_injury_related = any(k in lower for k in INJURY_KEYWORDS)
    is_il_placement = bool(IL_PLACEMENT_RE.search(text))
    is_il_transfer = bool(IL_TRANSFER_RE.search(text))
    is_il_activation = bool(IL_ACTIVATION_RE.search(text))
    is_rehab_assignment = bool(REHAB_ASSIGNMENT_RE.search(text))
    is_covid_il = "covid-19 injured list" in lower

    # "transfer" no es una nueva lesión; suele ser cambio 15->60 días.
    count_as_new_injury_registration = is_il_placement and not is_il_transfer

    if is_il_transfer:
        injury_event_type = "il_transfer"
    elif is_il_placement:
        injury_event_type = "il_placement"
    elif is_il_activation:
        injury_event_type = "il_activation"
    elif is_rehab_assignment:
        injury_event_type = "rehab_assignment"
    elif is_injury_related:
        injury_event_type = "injury_other"
    else:
        injury_event_type = "non_injury"

    il_days_bucket = None
    match = IL_DAY_CLASS_RE.search(text)
    if match:
        il_days_bucket = f"{match.group(1)}-day"
    elif "covid-19 injured list" in lower:
        il_days_bucket = "covid-19"
    elif "concussion injured list" in lower or "concussion disabled list" in lower:
        il_days_bucket = "concussion"

    return {
        "is_injury_related": int(is_injury_related),
        "injury_event_type": injury_event_type,
        "is_il_placement": int(is_il_placement),
        "is_il_activation": int(is_il_activation),
        "is_il_transfer": int(is_il_transfer),
        "is_rehab_assignment": int(is_rehab_assignment),
        "is_covid_il": int(is_covid_il),
        "il_days_bucket": il_days_bucket,
        "count_as_new_injury_registration": int(count_as_new_injury_registration),
    }


def flatten_transaction(tx: dict) -> Dict[str, object]:
    description = tx.get("description") or ""
    event_date = choose_event_date(tx)
    year = None
    if event_date:
        year = int(event_date[:4])

    flags = classify_injury(description)
    return {
        "transaction_id": tx.get("id"),
        "api_date": tx.get("date"),
        "effective_date": tx.get("effectiveDate"),
        "resolution_date": tx.get("resolutionDate"),
        "event_date": event_date,
        "year": year,
        "type_code": tx.get("typeCode"),
        "type_desc": tx.get("typeDesc"),
        "description": description,
        "person_id": safe_get(tx.get("person"), "id"),
        "person_name": safe_get(tx.get("person"), "fullName"),
        "from_team_id": safe_get(tx.get("fromTeam"), "id"),
        "from_team_name": safe_get(tx.get("fromTeam"), "name"),
        "to_team_id": safe_get(tx.get("toTeam"), "id"),
        "to_team_name": safe_get(tx.get("toTeam"), "name"),
        **flags,
    }


def daterange(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def write_csv(path: Path, rows: List[Dict[str, object]], columns: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in columns})


def build_daily_series(
    injury_rows: List[Dict[str, object]],
    start_date: date,
    end_date: date,
) -> List[Dict[str, object]]:
    by_day: Dict[str, Counter] = {}
    for row in injury_rows:
        d = row.get("event_date")
        if not d:
            continue
        if d not in by_day:
            by_day[d] = Counter()
        by_day[d]["injury_related_transactions"] += 1
        by_day[d]["injury_registrations"] += int(row.get("count_as_new_injury_registration", 0) or 0)
        by_day[d]["il_activations"] += int(row.get("is_il_activation", 0) or 0)
        by_day[d]["il_transfers"] += int(row.get("is_il_transfer", 0) or 0)
        by_day[d]["rehab_assignments"] += int(row.get("is_rehab_assignment", 0) or 0)

    rows: List[Dict[str, object]] = []
    for d in daterange(start_date, end_date):
        key = d.isoformat()
        c = by_day.get(key, Counter())
        rows.append(
            {
                "date": key,
                "year": d.year,
                "injury_registrations": int(c.get("injury_registrations", 0)),
                "injury_related_transactions": int(c.get("injury_related_transactions", 0)),
                "il_activations": int(c.get("il_activations", 0)),
                "il_transfers": int(c.get("il_transfers", 0)),
                "rehab_assignments": int(c.get("rehab_assignments", 0)),
            }
        )
    return rows


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start-year", type=int, default=2015)
    p.add_argument("--end-year", type=int, default=2025)
    p.add_argument("--outdir", type=Path, default=Path("data"))
    p.add_argument("--sleep", type=float, default=0.25, help="Pausa entre años")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.start_year > args.end_year:
        raise SystemExit("start-year debe ser <= end-year")

    all_rows: List[Dict[str, object]] = []
    for year in range(args.start_year, args.end_year + 1):
        start = f"{year}-01-01"
        end = f"{year}-12-31"
        url = build_url(start, end, sport_id=1)
        print(f"[INFO] Descargando {year}...", file=sys.stderr)
        payload = fetch_json(url)
        txs = payload.get("transactions", [])
        print(f"[INFO] {year}: {len(txs)} transacciones", file=sys.stderr)
        for tx in txs:
            all_rows.append(flatten_transaction(tx))
        time.sleep(args.sleep)

    # Orden por fecha/evento y id para trazabilidad
    all_rows.sort(key=lambda r: ((r.get("event_date") or ""), (r.get("transaction_id") or 0)))

    injury_rows = [r for r in all_rows if int(r.get("is_injury_related", 0) or 0) == 1]
    for r in injury_rows:
        r["count_as_new_injury_registration"] = int(r.get("count_as_new_injury_registration", 0) or 0)

    start_date = date(args.start_year, 1, 1)
    end_date = date(args.end_year, 12, 31)
    daily_rows = build_daily_series(injury_rows, start_date, end_date)

    raw_path = args.outdir / f"mlb_transactions_flat_{args.start_year}_{args.end_year}.csv"
    injury_path = args.outdir / f"mlb_injury_transactions_{args.start_year}_{args.end_year}.csv"
    daily_path = args.outdir / f"mlb_injuries_daily_{args.start_year}_{args.end_year}.csv"

    write_csv(raw_path, all_rows, RAW_COLUMNS)
    write_csv(injury_path, injury_rows, INJURY_COLUMNS)
    write_csv(daily_path, daily_rows, DAILY_COLUMNS)

    print("\n[OK] Archivos generados:", file=sys.stderr)
    print(f"  - {raw_path}", file=sys.stderr)
    print(f"  - {injury_path}", file=sys.stderr)
    print(f"  - {daily_path}", file=sys.stderr)
    print(f"  Total transacciones MLB: {len(all_rows)}", file=sys.stderr)
    print(f"  Eventos de lesión (IL/rehab): {len(injury_rows)}", file=sys.stderr)
    print(
        f"  Nuevas lesiones registradas (sum): "
        f"{sum(int(r.get('count_as_new_injury_registration', 0) or 0) for r in injury_rows)}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
