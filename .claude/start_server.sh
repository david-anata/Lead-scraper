#!/bin/bash
cd /Users/davidnarayan/Documents/Playground/Lead-scraper
exec python3 -m uvicorn sales_support_agent.main:app --port 8000 --reload
