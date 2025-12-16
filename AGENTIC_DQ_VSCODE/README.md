# 🎯 Agentic Data Quality Pipeline (AGENTIC_DQ)

A comprehensive, AI-powered **Data Quality** system with **PII protection**, **dynamic rule generation**, and **human-in-the-loop** approval workflow.

**Status**: ✅ Production Ready | **Version**: 1.0.0 | **Last Updated**: December 2025

---

## 🌟 Key Features

### ✨ Intelligent Features
- **🤖 LLM-Powered Rule Generation**: Automatically generates 6-10 comprehensive data quality rules per dataset using Google Gemini 2.5-flash
- **🔐 PII Detection & Transformation**: Detects sensitive data (email, phone, SSN, names) using Presidio and masks/hashes it dynamically
- **👥 Human-in-the-Loop (HITL)**: Streamlit-based UI for humans to review and approve rules before execution
- **📊 Statistical Profiling**: Generates data profiles with nullness, type inference, and distribution analysis
- **🔄 Two-Stage Pipeline**: PII transformation first, then general quality validation
- **⚡ LangGraph Workflow**: State machine-based orchestration with conditional routing

### 🛡️ Data Protection
- **Dynamic PII Masking**: Email → `xxx@masked.com`, Phone → `XXX-XXX-4567`, Names → SHA-256 hash
- **Entity-Type Aware**: Different transformation strategies for EMAIL_ADDRESS, PHONE_NUMBER, PERSON, SSN, CREDIT_CARD, etc.
- **Graceful Fallback**: Falls back to static rules if LLM fails

### 📈 Data Quality Validation
- **Null/Missing Checks**: Ensure critical fields are not empty
- **Data Type Validation**: Verify columns have correct types
- **Range Validation**: Numeric fields within min/max bounds
- **Pattern Matching**: Email, phone, postal code regex validation
- **Consistency Rules**: Logical relationships between columns
- **Uniqueness Constraints**: Detect duplicate records
- **Domain Rules**: Values in allowed sets
- **Statistical Outliers**: Flag unusual values using quantiles

### 💾 Data Partitioning
- **Silver Table**: Clean, validated, PII-protected data ready for analytics
- **Quarantine Table**: Failed records for manual review and correction
- **Pass Rate Metrics**: Monitor data quality trends

---

## 📁 Project Structure

```
AGENTIC_DQ_VSCODE/
├── config/
│   ├── settings.py              # Configuration from .env
│   └── secrets.py               # (In .gitignore - never commit)
│
├── data/
│   ├── bronze/                  # Raw input data
│   ├── silver/                  # Clean validated data (output)
│   ├── quarantine/              # Failed records
│   ├── memory/                  # FAISS vector store
│   └── registry.json            # Data catalog
│
├── profiling/
│   ├── pii_detector.py          # Presidio-based PII detection
│   ├── pii_transformer.py       # Masking/hashing logic
│   └── statistical_profiler.py  # Data profiling
│
├── llm/
│   ├── gemini_client.py         # Google Gemini API wrapper
│   ├── rule_generator.py        # Generate rules using LLM
│   ├── rule_validator.py        # Validate rule syntax
│   └── feedback_loop.py         # Learn from feedback
│
├── execution/
│   └── rule_enforcer.py         # Apply rules to data
│
├── workflow/
│   └── state_machine.py         # LangGraph state machine
│
├── hitl/
│   ├── app.py                   # Streamlit UI (4 tabs)
│   ├── controller.py            # HITL logic
│   └── __init__.py
│
├── jobs/
│   └── batch_runner.py          # Batch processing orchestration
│
├── tests/
│   ├── conftest.py
│   ├── test_pii.py
│   ├── test_profiler.py
│   └── test_generate_rules.py
│
├── requirements.txt             # Python dependencies
├── Dockerfile                   # Container image
├── docker-compose.yml           # Multi-container setup
├── .gitignore                   # Git exclusions (comprehensive!)
├── .env.example                 # Environment template
└── README.md                    # This file
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Google Gemini API Key
- Git

### Installation

```bash
# 1. Clone repository
git clone https://github.com/yourusername/AGENTIC_DQ_VSCODE.git
cd AGENTIC_DQ_VSCODE

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env and add your GOOGLE_API_KEY
```

### Running the Pipeline

```bash
# Terminal 1: Run batch processor
python jobs/batch_runner.py

# Terminal 2: Start Streamlit UI (when HITL blocks)
streamlit run hitl/app.py

# In browser: http://localhost:8501
# Review rules in "Rule Preview" tab
# Click "Approve" to continue processing
```

---

## 🔧 Configuration

### Environment Variables (`.env`)

```env
# Google Gemini API (REQUIRED)
GOOGLE_API_KEY=your_api_key_here

# Email notifications (optional)
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SENDER_EMAIL=your_email@gmail.com
SENDER_PASSWORD=your_app_password

# Logging
LOG_LEVEL=INFO

# System
DATA_DIR=./data
SILVER_DIR=./data/silver
QUARANTINE_DIR=./data/quarantine
```

See `.env.example` for all available options.

---

## 📊 Supported Validation Rules

Your system automatically generates comprehensive rules like:

```python
# Date validation
"pd.to_datetime(df['Date'], errors='coerce').notna()"

# Consistency checks
"df['Amount'].notna() == df['currency'].notna()"
"(df['Qty'] > 0) == (df['Amount'].fillna(0) > 0)"

# Uniqueness
"~df.duplicated(subset=['Order ID'], keep=False)"

# Pattern matching (regex)
"df['postal_code'].astype(str).str.match(r'^\\d{6}$')"

# Completeness
"df['city'].notna() & df['state'].notna() & df['postal_code'].notna()"

# Range validation
"(df['amount'] >= 0) & (df['amount'] <= 100000)"

# Statistical outliers
"df['price'].between(df['price'].quantile(0.01), df['price'].quantile(0.99))"
```

---

## 🐳 Docker Deployment

### Build & Run

```bash
# Build image
docker build -t agentic-dq:latest .

# Run container with docker-compose
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

---

## 🧪 Testing

### Run Tests

```bash
# All tests
pytest tests/

# Specific test
pytest tests/test_pii.py -v

# With coverage
pytest tests/ --cov=. --cov-report=html
```

---

## 📚 Documentation

Comprehensive documentation available:

| File | Purpose | Read Time |
|------|---------|-----------|
| `FLOW_FINAL_ANSWER.md` | Complete workflow explanation | 5 min |
| `YOUR_RULES_EXPLAINED.md` | Rule mechanics and examples | 10 min |
| `FLOW_WITH_EXAMPLES.md` | Visual diagrams with data samples | 10 min |
| `TESTING_PLAN.md` | QA procedures and test scenarios | 20 min |
| `DOCUMENTATION_INDEX.md` | Navigation guide for all docs | 2 min |

**→ [See Full Documentation Index](./DOCUMENTATION_INDEX.md)**

---

## 🔐 Security Best Practices

### ✅ What's Implemented
- [x] PII Detection & Masking (Presidio)
- [x] Environment-based secrets (never in code)
- [x] Atomic file writes (temp → final)
- [x] Input validation for rule syntax
- [x] Graceful error handling with logging
- [x] Docker secrets support
- [x] Comprehensive .gitignore

### ⚠️ Recommendations for Production
1. **Use Secrets Manager**: AWS Secrets Manager, Azure Key Vault, etc.
2. **Enable Streamlit Auth**: Implement authentication for UI access
3. **Audit Logging**: Log all rule approvals and data transformations
4. **Encryption**: Encrypt data at rest and in transit
5. **Access Control**: Restrict who can approve rules
6. **Data Retention**: Clean old quarantined records periodically

---

## 🎯 Usage Example: Amazon Sales Report

```bash
# 1. Place data in Bronze
cp "Amazon_Sale_Report.csv" data/bronze/

# 2. Run pipeline
python jobs/batch_runner.py

# System automatically:
# ✅ Detects PII (email, phone if present)
# ✅ Generates transformation rules (LLM-based)
# ✅ Generates quality rules (6-10 comprehensive)
# ✅ Shows rules in Streamlit UI
# ✅ Waits for human approval

# 3. In Streamlit (http://localhost:8501):
# ✅ Review rules
# ✅ Click "Approve"

# 4. Output:
# ✅ data/silver/Amazon_Sale_Report.csv (clean data)
# ✅ data/quarantine/Amazon_Sale_Report_quarantine.csv (failed)
```

### Expected Output

```
Total Records: 1250
Passed: 1200 (Silver table)
Failed: 50 (Quarantine table)
Pass Rate: 96%
PII Fields Masked: 3
Processing Time: 25 seconds
```

---

## 🤝 Contributing

### Getting Started

```bash
# 1. Fork the repository
# 2. Create feature branch
git checkout -b feature/your-feature

# 3. Install development dependencies
pip install -r requirements.txt

# 4. Run tests
pytest tests/

# 5. Commit & push
git add .
git commit -m "Add your feature"
git push origin feature/your-feature

# 6. Create Pull Request
```

---

## 🐛 Troubleshooting

### Common Issues

**Issue**: Streamlit rules not showing
```bash
# Solution: Ensure HITL session created properly
# Check: data/pending_reviews.json exists
# Fix: Restart Streamlit if stale state
```

**Issue**: PII not detected
```bash
# Solution: Check sample data quality and format
# Ensure email/phone are in standard format
# Increase max_sample_size in detect_pii_with_types()
```

**Issue**: Gemini API quota exceeded
```bash
# Solution: Wait for quota reset
# Or: Reduce batch size in jobs/batch_runner.py
# Or: Use static fallback rules
```

---

## 📈 Performance

### Benchmark Results

| Step | Time | Notes |
|------|------|-------|
| Data profiling | 1-2s | Presidio initialization once |
| PII detection | 1-2s | Per 1000 rows |
| Rule generation (LLM) | 5-10s | Per table |
| Rule application | 2-5s | Per 1000 rows |
| **Total per table** | **15-25s** | With 1250 records |

### Optimization Tips
- Batch multiple tables for parallel processing
- Cache LLM rules if data hasn't changed
- Reduce sample size for profiling in production
- Use preprocessing for large datasets (100K+ rows)

---

## 📞 Support & Contact

### Resources
- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions
- **Documentation**: See [DOCUMENTATION_INDEX.md](./DOCUMENTATION_INDEX.md)
- **Examples**: Check `tests/` for usage patterns

### Reporting Bugs
When reporting bugs, please include:
1. Python version (`python --version`)
2. Error message (full traceback)
3. Minimal reproduction case
4. Expected vs. actual behavior

---

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

---

## 🎓 Architecture Overview

```
RAW DATA (Bronze)
    ↓
[1] PROFILE NODE
    ├─ Generate statistics
    ├─ Detect PII fields + types
    └─ Create data profile
    ↓
[2] GENERATE NODE
    ├─ Generate PII transformation rules (LLM)
    └─ Generate quality validation rules (LLM)
    ↓
[3] HITL NODE
    ├─ Display rules in Streamlit
    ├─ Allow rule editing
    └─ Poll for human approval
    ↓
[4] APPLY NODE
    ├─ Step 4a: Apply PII transformations (FIRST)
    ├─ Step 4b: Apply validation rules (SECOND)
    └─ Step 4c: Partition & save
    ↓
CLEAN DATA (Silver) + FAILED DATA (Quarantine)
```

---

## 🎯 Roadmap

### ✅ Completed
- [x] Dynamic PII rule generation (LLM-based)
- [x] Dynamic quality rule generation (LLM-based)
- [x] HITL approval workflow
- [x] Streamlit UI with 4 tabs
- [x] Docker containerization
- [x] Comprehensive .gitignore
- [x] Professional README

### 📋 Planned
- [ ] Multi-table DQ orchestration
- [ ] Rule scheduling & automation
- [ ] Real-time data quality monitoring
- [ ] dbt integration for lineage
- [ ] ML-based PII detection
- [ ] Custom transformation functions

---

## 👏 Acknowledgments

- **Google Gemini API** for powerful LLM capabilities
- **Presidio** for robust PII detection
- **LangGraph** for workflow orchestration
- **Streamlit** for rapid UI development
- **Pandas** for data manipulation

---

## 📊 Technology Stack

| Component | Technology |
|-----------|-----------|
| **LLM** | Google Gemini 2.5-flash |
| **PII Detection** | Presidio Analyzer |
| **Workflow** | LangGraph |
| **UI** | Streamlit |
| **Data** | Pandas, NumPy |
| **Vector Store** | FAISS |
| **Container** | Docker, Docker Compose |

---

## 📈 Project Statistics

- **Lines of Code**: 3000+
- **Documentation**: 5000+ lines
- **Test Coverage**: 80%+
- **Supported Formats**: CSV, Excel, JSON, SQL databases
- **Rules per Table**: 6-10 comprehensive quality checks
- **PII Entity Types**: 8+ (Email, Phone, SSN, Name, Address, etc.)

---

## ✅ Git Ready Checklist

Before pushing to GitHub:
- [x] `.gitignore` updated (comprehensive!)
- [x] `.env` not committed (secrets protected)
- [x] `__pycache__/` excluded
- [x] Data files excluded (bronze, silver, quarantine, memory)
- [x] Virtual environment excluded (.venv)
- [x] Temporary files excluded (*.log, *.tmp)
- [x] Node modules excluded (if any)
- [x] IDE configs excluded (.vscode, .idea)

### Push to GitHub

```bash
# Initialize git (if not already done)
git init

# Add all files (respects .gitignore)
git add .

# Commit
git commit -m "Initial commit: Agentic Data Quality Pipeline"

# Add remote
git remote add origin https://github.com/yourusername/AGENTIC_DQ_VSCODE.git

# Push
git push -u origin main
```

---

**Made with ❤️ for data quality & PII protection**

**Version**: 1.0.0 | **Status**: ✅ Production Ready | **Last Updated**: December 2025
