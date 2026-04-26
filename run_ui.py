#!/usr/bin/env python3
import os, sys
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())
from ui.backend.app import run_ui
run_ui(config_path="config.test.yml", port=7070, open_browser=False)
