# RFQAI 🚀

> **Intelligent RFQ Processing with AI-Powered Document Understanding**

A production-grade system for automated Request for Quotation (RFQ) document processing, extraction, and intelligent querying using advanced AI embeddings and RAG (Retrieval-Augmented Generation).

---

## ✨ Features

- **🤖 Intelligent Document Processing**: Automatically extract structured data from RFQ documents using vision models and OCR
- **🔍 Semantic Search**: Find relevant information across documents using AI-powered embeddings with pgvector
- **📊 Multi-Format Support**: Handle PDFs, Excel spreadsheets, images, CSVs, PowerPoint, and Word documents
- **🔗 Seamless Integration**: Connect with Google Drive, Glide CRM, and custom databases
- **⚡ Scalable Pipeline**: Built with LangGraph for efficient, distributed document processing
- **🗂️ Database-Driven**: PostgreSQL-based storage with pgvector for semantic search capabilities
- **📝 Normalized Data**: Automatic document normalization and schema validation

---

## 🏗️ Architecture

```
RFQAI
├── Document Ingestion
│   ├── File Extraction (PDF, Excel, Images, etc.)
│   ├── OCR & Vision Processing
│   └── Format Normalization
├── Processing Pipeline
│   ├── Document Building & Chunking
│   ├── Vector Embeddings (Gemini)
│   └── Database Upsertion
├── Retrieval & Query
│   ├── Semantic Search
│   ├── Reranking
│   └── Evidence Compilation
└── API Layer
    ├── Health Checks
    ├── Ingestion Endpoints
    └── Query Interface
```

### Key Components

- **Pipeline**: LangGraph-based workflow orchestration (`pipeline/ingest_graph.py`, `query_graph.py`)
- **File Extractors**: Multi-format document parsing (`tools/file_extractors/`)
- **Embeddings**: Gemini-powered vector generation with pgvector storage
- **RAG System**: Retriever, reranker, and evidence builder (`rag/`)
- **Database**: PostgreSQL with custom schema and indexing (`packages/db/`)

---

## 🛠️ Tech Stack

### Core
- **Framework**: FastAPI + Uvicorn
- **Orchestration**: LangGraph
- **Database**: PostgreSQL + pgvector
- **Embeddings**: Google Gemini
- **File Processing**: PyPDF, pdfplumber, Pillow, openpyxl, python-pptx, python-docx

### Integration
- **Google Drive API**: For document fetching and management
- **Glide CRM**: For RFQ data synchronization
- **Custom Databases**: Via db_tool interface

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL with pgvector extension
- Docker (optional)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/aniketsandhanwootz-wq/RFQAI.git
   cd RFQAI
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   cd service
   pip install -r requirements.txt
   ```

4. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Run migrations**
   ```bash
   psql -U postgres -d rfqai -f packages/db/migrations/001_extensions.sql
   psql -U postgres -d rfqai -f packages/db/migrations/002_rfq_schema.sql
   psql -U postgres -d rfqai -f packages/db/migrations/003_indexes.sql
   ```

6. **Start the service**
   ```bash
   uvicorn app.main:app --reload
   ```

The API will be available at `http://localhost:8000` with interactive docs at `/docs`

---

## 📋 Environment Configuration

Create a `.env` file in the service directory:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/rfqai
DB_SCHEMA=rfq

# Embeddings (Google Gemini)
GEMINI_API_KEY=your_gemini_api_key
GEMINI_EMBED_MODEL=gemini-embedding-001
EMBED_DIM=1536

# Ingestion
INGEST_HTTP_TIMEOUT_SEC=60
INGEST_FILE_MAX_MB=40

# Chunking
CHUNK_SIZE=1200
CHUNK_OVERLAP=150

# Glide CRM Integration
GLIDE_API_KEY=your_glide_api_key
GLIDE_APP_ID=your_glide_app_id
GLIDE_MAX_ROWS_PER_CALL=5000

# Google Drive Integration
GDRIVE_SA_JSON_PATH=/path/to/service-account.json

# Logging
LOG_LEVEL=INFO
```

---

## 📚 API Endpoints

### Health Check
```http
GET /health
```
Returns service health status.

### Document Ingestion
```http
POST /ingest/documents
```
Ingest and process RFQ documents.

**Request Body:**
```json
{
  "source": "google_drive",
  "file_ids": ["file1", "file2"],
  "sync_to_glide": true
}
```

### Query & Search
```http
POST /query
```
Search and retrieve relevant information from ingested documents.

**Request Body:**
```json
{
  "query": "What is the lead time for delivery?",
  "top_k": 5,
  "rerank": true
}
```

---

## 🔄 Processing Pipeline

### Ingestion Flow
1. **Fetch**: Documents from Google Drive or external sources
2. **Extract**: Parse files (PDF, Excel, Images, etc.)
3. **Normalize**: Apply schema validation and data normalization
4. **Chunk**: Split documents into processable segments
5. **Embed**: Generate vector embeddings using Gemini
6. **Store**: Upsert into PostgreSQL with pgvector

### Query Flow
1. **Embed**: Convert query to vector embedding
2. **Search**: Semantic similarity search in pgvector
3. **Rerank**: Score and rank results by relevance
4. **Compile**: Build evidence with supporting context
5. **Return**: Structured response with citations

---

## 📁 Project Structure

```
service/
├── app/
│   ├── main.py                 # FastAPI application entry
│   ├── config.py               # Settings management
│   ├── integrations/           # External service clients
│   ├── pipeline/               # LangGraph workflows
│   ├── rag/                    # Retrieval & ranking
│   ├── routers/                # API endpoints
│   ├── scripts/                # CLI utilities
│   └── tools/                  # Core tools & extractors
├── requirements.txt            # Python dependencies
└── Dockerfile                  # Container configuration

packages/
├── contracts/                  # Schema definitions (YAML)
├── db/                        # Database migrations & setup
└── prompts/                   # LLM prompt templates
```

---

## 🧪 Testing

Run the test suite:

```bash
pytest tests/ -v
```

Key tests:
- `test_embed_dim.py`: Embedding dimension validation
- `test_idempotency.py`: Pipeline idempotency verification

---

## 🐳 Docker Deployment

Build and run with Docker:

```bash
docker build -t rfqai:latest .
docker run -p 8000:8000 \
  --env-file .env \
  -v /path/to/data:/data \
  rfqai:latest
```

---

## 📖 Documentation

- [Architecture Guide](docs/architecture.md) - System design and data flow
- [Operations Guide](docs/ops.md) - Deployment and troubleshooting
- [Database Schema](packages/db/migrations/) - Schema definitions

---

## 🔐 Security

- Environment variables for sensitive credentials
- Database connection pooling with authentication
- API request validation with Pydantic
- File size limits to prevent abuse
- Timeout controls for external service calls

---

## 📈 Performance

- **Vector Search**: Fast semantic search with pgvector indexing
- **Batch Processing**: Handle multiple documents in parallel with LangGraph
- **Caching**: Results caching for frequently queried documents
- **Connection Pooling**: Efficient database connection management

---

## 🤝 Contributing

1. Create a feature branch (`git checkout -b feature/amazing-feature`)
2. Commit changes (`git commit -m 'Add amazing feature'`)
3. Push to branch (`git push origin feature/amazing-feature`)
4. Open a Pull Request

---

## 📄 License

This project is proprietary. All rights reserved.

---

## 🆘 Support

For issues, questions, or suggestions:
- Open an GitHub issue
- Check existing documentation in `/docs`
- Review the architecture guide for system design questions

---

## 🎯 Roadmap

- [ ] Multi-language document support
- [ ] Advanced entity extraction
- [ ] Custom LLM integration options
- [ ] Real-time document streaming
- [ ] Enhanced visualization dashboards
- [ ] GraphQL API layer

---

## 📊 Key Metrics

- **Document Processing**: Supports up to 40MB per file
- **Embedding Dimension**: 1536 (Gemini standard)
- **Chunk Size**: 1200 tokens (configurable)
- **Glide Integration**: Up to 5000 rows per sync
- **HTTP Timeout**: 60 seconds (configurable)

---

**Built with ❤️ for intelligent document processing**
