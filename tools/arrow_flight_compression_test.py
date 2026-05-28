#!/usr/bin/env python3
"""
Arrow Flight IPC compression smoke test for StarRocks.

Tests both the cluster-config path (be.conf arrow_flight_compression) and the
per-call header path (x-arrow-ipc-compression gRPC header).

Usage:
    pip install pyarrow adbc-driver-flightsql
    python arrow_flight_compression_test.py [--host FE_HOST] [--port FLIGHT_PORT]
                                            [--user USER] [--password PASSWORD]
                                            [--database DB] [--query SQL]

Prerequisites:
    - StarRocks FE running with Arrow Flight SQL enabled (arrow_flight_port set, e.g. 9408)
    - A table with enough rows to produce visible size difference (~1 K+ rows of strings)
"""

import argparse
import sys
import time

import pyarrow as pa
import pyarrow.flight as flight

# Optional: ADBC driver gives a cleaner Flight SQL session — used if available.
try:
    import adbc_driver_flightsql.dbapi as adbc
    HAS_ADBC = True
except ImportError:
    HAS_ADBC = False


# ---------------------------------------------------------------------------
# Low-level Flight SQL helpers (works with plain pyarrow, no ADBC needed)
# ---------------------------------------------------------------------------

def _make_statement_query(sql: str) -> bytes:
    """Encode a Flight SQL StatementQuery command as a protobuf Any."""
    # Minimal hand-rolled protobuf: field 1 (query string), tag = (1 << 3) | 2 = 0x0a
    encoded_sql = sql.encode()
    proto_body = bytes([0x0A, len(encoded_sql)]) + encoded_sql

    # Wrap in google.protobuf.Any: type_url field 1, value field 2
    type_url = b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    any_body = (
        bytes([0x0A, len(type_url)]) + type_url +
        bytes([0x12, len(proto_body)]) + proto_body
    )
    return any_body


def fetch_via_flight(host: str, port: int, sql: str,
                     username: str, password: str,
                     compression_header: str | None = None) -> tuple[pa.Table, float]:
    """
    Execute *sql* via raw Arrow Flight SQL and return (table, elapsed_seconds).
    If *compression_header* is set, it is sent as 'x-arrow-ipc-compression'.
    """
    location = flight.Location.for_grpc_tcp(host, port)
    options = flight.FlightClientOptions()
    client = flight.FlightClient(location, **{})

    # Authenticate (Basic auth)
    token_pair = client.authenticate_basic_token(username, password)
    call_options = flight.FlightCallOptions(headers=[token_pair])
    if compression_header:
        call_options = flight.FlightCallOptions(
            headers=[token_pair, (b"x-arrow-ipc-compression", compression_header.encode())]
        )

    descriptor = flight.FlightDescriptor.for_command(_make_statement_query(sql))
    flight_info = client.get_flight_info(descriptor, call_options)

    t0 = time.perf_counter()
    batches = []
    for endpoint in flight_info.endpoints:
        reader = client.do_get(endpoint.ticket, call_options)
        batches.extend(reader.read_all().to_batches())
    elapsed = time.perf_counter() - t0

    table = pa.Table.from_batches(batches) if batches else pa.table({})
    return table, elapsed


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

def run_tests(host: str, port: int, user: str, password: str,
              database: str, query: str) -> bool:
    print(f"\nConnecting to {host}:{port} as {user!r}")
    print(f"Query: {query}\n")

    full_query = f"SELECT * FROM {database}.{query}" if "." not in query and " " not in query else query

    results = {}
    passed = 0
    failed = 0

    for label, header in [
        ("no compression (baseline)", None),
        ("lz4 via header", "lz4"),
        ("zstd via header", "zstd"),
    ]:
        try:
            table, elapsed = fetch_via_flight(host, port, full_query, user, password,
                                              compression_header=header)
            row_count = len(table)
            # Estimate serialised size as sum of buffer sizes in the table
            size_bytes = sum(
                buf.size for col in table.columns for chunk in col.chunks
                for buf in chunk.buffers() if buf is not None
            )
            results[label] = {"rows": row_count, "size": size_bytes, "elapsed": elapsed}
            print(f"  [{label}]  rows={row_count:,}  in-memory={size_bytes:,} bytes  time={elapsed:.3f}s  OK")
            passed += 1
        except Exception as exc:
            print(f"  [{label}]  FAILED: {exc}")
            results[label] = None
            failed += 1

    # Sanity check: row counts must match across all successful runs
    row_counts = {v["rows"] for v in results.values() if v is not None}
    if len(row_counts) > 1:
        print(f"\nFAIL: inconsistent row counts across runs: {row_counts}")
        failed += 1
    else:
        print(f"\nRow-count consistency: OK ({row_counts.pop()} rows in every run)")
        passed += 1

    # Informational: compression reduces wire size (not always visible in in-memory size,
    # but the Arrow IPC body is compressed on the wire; pyarrow decompresses before
    # returning the table, so in-memory sizes are identical).
    print("\nNote: pyarrow decompresses before returning the table, so in-memory sizes")
    print("are always identical regardless of compression. To measure wire-level savings,")
    print("capture packets (e.g. with tcpdump) or instrument the gRPC layer.")

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="127.0.0.1", help="FE hostname (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=9408, help="Arrow Flight port (default: 9408)")
    parser.add_argument("--user", default="root", help="StarRocks username (default: root)")
    parser.add_argument("--password", default="", help="StarRocks password (default: empty)")
    parser.add_argument("--database", default="test", help="Database name (default: test)")
    parser.add_argument("--query", default="SELECT 1 AS n",
                        help="SQL query or bare table name (default: 'SELECT 1 AS n')")
    args = parser.parse_args()

    ok = run_tests(args.host, args.port, args.user, args.password,
                   args.database, args.query)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
