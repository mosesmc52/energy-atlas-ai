#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test agents.llm_query_parser independent of Chainlit."
    )
    parser.add_argument(
        "query",
        nargs="*",
        help="Query to parse. If omitted, starts an interactive prompt.",
    )
    parser.add_argument(
        "--model",
        help=(
            "Override ATLAS_QUERY_PARSER_MODEL for this run. "
            "Defaults to ATLAS_QUERY_PARSER_MODEL, ATLAS_ROUTER_MODEL, or gpt-5.2."
        ),
    )
    parser.add_argument(
        "--show-plan",
        action="store_true",
        help="Also show the source plan produced from the parsed output.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Print compact JSON instead of indented JSON.",
    )
    return parser.parse_args()


def parse_and_print(query: str, *, show_plan: bool, compact: bool) -> None:
    from agents.llm_query_parser import llm_parse_query
    from agents.router import normalize_query, route_query

    normalized_query = normalize_query(query)
    parsed = llm_parse_query(user_query=query, normalized_query=normalized_query)
    route = route_query(query)
    output: dict[str, Any] = {
        "query": query,
        "normalized_query": normalized_query,
        "parser_output": parsed,
        "route": route,
    }
    if show_plan:
        from agents.source_planner import build_source_plan

        output["source_plan"] = build_source_plan(route)

    json_kwargs = {"default": _jsonable, "sort_keys": True}
    if not compact:
        json_kwargs["indent"] = 2
    print(json.dumps(output, **json_kwargs))


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")
    args = parse_args()
    if args.model:
        os.environ["ATLAS_QUERY_PARSER_MODEL"] = args.model

    query = " ".join(args.query).strip()
    try:
        if query:
            parse_and_print(query, show_plan=args.show_plan, compact=args.compact)
            return 0

        print("Enter a query to parse. Press Ctrl-D or submit an empty line to exit.")
        while True:
            try:
                query = input("> ").strip()
            except EOFError:
                print()
                return 0
            if not query:
                return 0
            parse_and_print(query, show_plan=args.show_plan, compact=args.compact)
    except Exception as exc:  # noqa: BLE001
        print(f"LLM query parser failed: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print()
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
