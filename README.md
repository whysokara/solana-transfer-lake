# Solana Transfer Lake

An end-to-end data engineering pipeline for analyzing high-frequency Solana payment and transfer activity.

## Overview

This project ingests real Solana on-chain transfer data, cleans and standardizes it using Apache Spark, and produces analytics-ready daily aggregates.

The pipeline follows a Bronze → Silver → Gold data lake architecture and is designed to be production-ready and cloud-portable.

## Data Flow

1. **Bronze (Raw)**
   - Source: Helius Solana API
   - Format: CSV
   - Content: Parsed wallet-to-wallet transfers

2. **Silver (Cleaned)**
   - Format: Parquet
   - Explicit schema enforcement
   - Filters only valid payment events
   - Adds event_date derived from timestamp

3. **Gold (Aggregated)**
   - Daily aggregates per event_date
   - Metrics:
     - Total transactions
     - Total transferred amount
     - Unique senders
     - Unique receivers

## Tech Stack

- Python 3.10
- Apache Spark (PySpark)
- Helius Solana API
- Local filesystem (S3-ready layout)
- Virtual environment based dependency isolation

## Project Structure

