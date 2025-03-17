# Indeed Job Scraper for South Africa

This Python script scrapes job listings from Indeed South Africa (za.indeed.com) using asynchronous Playwright. It handles CloudFlare checks, scrolls through pages, and extracts job details into a structured format.

## Features

- **Multi-instance scraping**: Launches multiple browser instances to scrape in parallel
- **CloudFlare Bypass**: Automatically handles CloudFlare security checks
- **Dynamic Scrolling**: Scrolls through pages and detects job listing links
- **Data Deduplication**: Uses thread-safe sets to avoid duplicate entries
- **Job Details Extraction**: Captures title, company, location, description, and job type

## Prerequisites

- Python 3.8+
- Playwright (with Chromium)
- Required libraries: `playwright`, `pandas`, `asyncio`

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/indeed-scraper.git
   cd indeed-scraper

## Dependencies 

pip install playwright pandas asyncio
playwright install chromium