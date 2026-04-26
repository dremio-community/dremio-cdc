#!/usr/bin/env python3
"""
Dremio CDC — entry point.

Usage:
    python main.py --config config.yml               # headless daemon
    python main.py --ui --config config.yml          # open the web UI
    python main.py --ui --port 7070 --no-browser     # UI, no auto-open
    python main.py --config config.yml --log-level DEBUG
"""
import argparse
import logging
import sys

from core.config import load_config


def main():
    parser = argparse.ArgumentParser(description="Dremio CDC daemon / UI")
    parser.add_argument("--config", default="config.yml", help="Path to config YAML")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--ui", action="store_true", help="Launch the web UI instead of headless mode")
    parser.add_argument("--port", type=int, default=7070, help="UI server port (default 7070)")
    parser.add_argument("--no-browser", action="store_true", help="Don't auto-open browser")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.ui:
        from ui.backend.app import run_ui
        run_ui(config_path=args.config, port=args.port, open_browser=not args.no_browser)
        return

    # Headless daemon mode
    from core.engine import CDCEngine
    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        sys.exit(f"Config file not found: {args.config}")

    engine = CDCEngine(cfg)
    engine.start()
    engine.join()


if __name__ == "__main__":
    main()
