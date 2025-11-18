#!/usr/bin/env python3
"""
Utility to find bulk RNA-seq series run on Illumina HiSeq 2500 from NCBI GEO.

The script uses the NCBI E-utilities (esearch + esummary) to locate relevant
datasets in the GDS database and prints a compact JSON summary.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Iterable, List, Sequence

import requests

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


@dataclass
class GeoRecord:
    accession: str
    title: str
    organism: str
    platform: str
    gds_type: str
    samples: int
    summary: str


def build_term(
    platform_keyword: str,
    gds_type: str,
    instrument_keywords: Sequence[str],
    organism: str | None,
) -> str:
    """Construct an Entrez search term."""
    clauses: List[str] = []

    if platform_keyword:
        clauses.append(f'"{platform_keyword}"[Platform]')

    if instrument_keywords:
        instrument_expr = " OR ".join(
            f'"{keyword}"[All Fields]' for keyword in instrument_keywords
        )
        clauses.append(f"({instrument_expr})")

    if gds_type:
        clauses.append(f'"{gds_type}"[gdsType]')

    if organism:
        clauses.append(f'"{organism}"[Organism]')

    if not clauses:
        raise ValueError("At least one clause must be specified for the search term.")

    return " AND ".join(clauses)


def esearch_ids(term: str, retmax: int) -> list[str]:
    """Call esearch to retrieve GDS IDs."""
    resp = requests.get(
        f"{EUTILS_BASE}/esearch.fcgi",
        params={
            "db": "gds",
            "term": term,
            "retmax": retmax,
            "retmode": "json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["esearchresult"].get("idlist", [])


def esummary_records(ids: Iterable[str]) -> list[GeoRecord]:
    """Retrieve summaries for each GDS ID."""
    id_list = list(ids)
    if not id_list:
        return []

    resp = requests.get(
        f"{EUTILS_BASE}/esummary.fcgi",
        params={
            "db": "gds",
            "id": ",".join(id_list),
            "retmode": "json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()["result"]

    records = []
    for gid in id_list:
        entry = result.get(gid)
        if not entry:
            continue

        records.append(
            GeoRecord(
                accession=entry.get("acc", ""),
                title=entry.get("title", ""),
                organism=entry.get("taxon", ""),
                platform=entry.get("platform", ""),
                gds_type=entry.get("gdsType", ""),
                samples=int(entry.get("samples", 0)),
                summary=entry.get("summary", ""),
            )
        )

    return records


def to_dict(record: GeoRecord) -> dict:
    """Convert record to JSON-serializable dict."""
    return {
        "accession": record.accession,
        "title": record.title,
        "organism": record.organism,
        "platform": record.platform,
        "gds_type": record.gds_type,
        "samples": record.samples,
        "summary": record.summary,
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch bulk RNA-seq GEO series for Illumina HiSeq 2500."
    )
    parser.add_argument(
        "--organism",
        default="Homo sapiens",
        help="Organism filter for the search term (default: %(default)s).",
    )
    parser.add_argument(
        "--retmax",
        type=int,
        default=20,
        help="Maximum number of records to retrieve (default: %(default)s).",
    )
    parser.add_argument(
        "--out",
        default="-",
        help="Output file path (default: stdout).",
    )
    parser.add_argument(
        "--platform",
        default="Illumina HiSeq 2500",
        help="Platform keyword constraint (default: %(default)s).",
    )
    parser.add_argument(
        "--gds-type",
        default="Expression profiling by high throughput sequencing",
        help="GDS dataset type filter (default: %(default)s).",
    )
    parser.add_argument(
        "--keyword",
        action="append",
        default=["bulk RNA-seq", "RNA-Seq"],
        help="Additional instrument keywords. Can be passed multiple times.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    term = build_term(
        platform_keyword=args.platform,
        gds_type=args.gds_type,
        instrument_keywords=args.keyword,
        organism=args.organism,
    )

    ids = esearch_ids(term, args.retmax)
    records = esummary_records(ids)
    payload = {
        "query": term,
        "count": len(records),
        "records": [to_dict(rec) for rec in records],
    }

    data = json.dumps(payload, indent=2)
    if args.out == "-" or args.out.lower() == "stdout":
        print(data)
    else:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(data)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

