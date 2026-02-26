# RFQAI 🚀

> **AI-Powered RFQ Intelligence Platform**  
> Intelligent extraction, understanding, and semantic search for RFQ documents with Glide CRM integration

[![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.133+-green?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.2+-orange?logo=langchain&logoColor=white)](https://www.langchain.com/)

---

## 🎯 Overview

RFQAI is an enterprise-grade system for **intelligent RFQ document processing and semantic search**. It seamlessly integrates with **Glide CRM**, automatically extracts information from **multi-format documents**, and enables powerful **AI-driven search capabilities** using advanced embeddings and vector databases.

### 🔄 Recent Updates
- Enhanced file routing: MIME guessing and normalized filenames to improve extractor accuracy
- Expanded image format support and smarter file type detection
- Updated `requirements.txt` with newer LangChain ecosystem versions (see note in installation)
- Added detailed ingestion readiness and architecture documentation



### What It Does ✅

- **Glide CRM Sync**: Read 4 Glide tables (RFQs, products, queries, shares) and sync to PostgreSQL schema `rfq.*`
- **Multi-Format Extraction**: Parse PDFs, XLSX, CSV, DOCX, PPTX, and images with intelligent fallback
- **Vision Intelligence**: Gemini Vision API processes scanned PDFs, diagrams, embedded images, and complex layouts
- **Semantic Indexing**: Auto-chunk and embed documents using Gemini (1536-dim vectors) stored in pgvector
- **Scalable Pipeline**: LangGraph-based orchestration for reliable, resumable processing
- **Production Ready**: Full error handling, transaction support, and comprehensive logging

### What's Coming Soon 🚧

- RAG API for intelligent querying and retrieval
- Microsoft Graph integration (architecture supports multiple providers)
- Advanced reranking for search results
- Real-time streaming ingestion

---

## 🏗️ Architecture:

```
┌─────────────────────────────────────────────────────────┐
│                    RFQAI System                         │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐       ┌──────────────┐              │
│  │  Glide CRM   │──────▶│  PostgreSQL  │              │
│  │  (RFQ Data)  │       │  (rfq.*)     │              │
│  └──────────────┘       └──────────────┘              │
│         │                       ▲                      │
│         │                       │                      │
│  ┌──────▼──────────────────┐    │                     │
│  │  Ingestion Pipeline     │    │                     │
│  │  (LangGraph 8-Stage)    │────┘                     │
│  └──────────────────────────┘                         │
│         │                                              │
│    [Phase 1-2]  [Phase 3]      [Phase 4]             │
│   Load & Build  Extract Files  Process & Store      │
│         │         │                │                  │
│  ┌──────▼─────────▼──────────────▼──┐               │
│  │  File Extractors                  │               │
│  │  ┌─────┬──────┬───┬──────┬──────┐│              │
│  │  │ PDF │ XLSX │CSV│PPTX │DOCX ││              │
│  │  │ +   │ +    │   │ +   │ +   ││              │
│  │  │Vision│Vision│   │Vision│Vision││              │
│  │  └─────┴──────┴───┴──────┴──────┘│              │
│  └──────────────────────────────────┘              │
│         │                                            │
│  ┌──────▼────────────────────┐                     │
│  │  Gemini Embeddings        │                     │
│  │  (1536-dim vectors)       │                     │
│  └──────┬────────────────────┘                     │
│         │                                            │
│  ┌──────▼────────────────────┐                     │
│  │  pgvector (Semantic Index)│                     │
│  │  (rfq.chunks)             │                     │
│  └───────────────────────────┘                     │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

## 📊 Key Features

| Feature | Details |
|---------|---------|
| **Multi-Format Support** | PDF, XLSX, CSV, DOCX, PPTX, PNG, JPG, WEBP + more (auto-detected via MIME/filename) |
| **Vision Processing** | Gemini Vision for scanned docs, diagrams, and embedded images |
| **Semantic Search** | pgvector indexing for fast similarity search |
| **Glide Integration** | Real-time sync with Glide CRM tables |
| **Google Drive** | Automatic file crawling and download |
| **Idempotent** | Safe to re-run; handles duplicates gracefully |
| **Smart Routing** | MIME type guessing and filename heuristics for accurate extractor selection |
| **Configurable** | Chunk size, embedding dims, timeout, file limits all tunable |
| **Production-Grade** | Full error handling, logging, transaction support |

---

## 🚀 Quick Start

### Prerequisites
- **Python**: 3.10+
- **PostgreSQL**: 12+ with pgvector extension
- **API Keys**: Gemini (embeddings), Glide CRM, Google Drive SA

### Installation

#### 1️⃣ Clone Repository
```bash
git clone https://github.com/aniketsandhanwootz-wq/RFQAI.git
cd RFQAI
```

#### 2️⃣ Create Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

#### 3️⃣ Install Dependencies
```bash
cd service
pip install -U pip
pip install -r requirements.txt
# note: requirements include langgraph 0.2.62 and langchain-core 0.3.83
# ensure compatibility when upgrading the LangChain ecosystem
cd ..
```

#### 4️⃣ Setup Database
```bash
# Create PostgreSQL database
createdb rfqai

# Install pgvector extension
psql -d rfqai -c "CREATE EXTENSION IF NOT EXISTS vector;"

# Run migrations
psql -d rfqai -f packages/db/migrations/001_extensions.sql
psql -d rfqai -f packages/db/migrations/002_rfq_schema.sql
psql -d rfqai -f packages/db/migrations/003_indexes.sql
```