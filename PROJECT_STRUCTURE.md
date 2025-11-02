# Project Structure

## Core Files

### Main Application
- **`lead_pipeline.py`** - Main pipeline orchestrator with all business logic
- **`queue_worker.py`** - Queue processor for Supabase enrichment requests
- **`log_capture.py`** - Log capture and storage functionality

### Testing
- **`test_suite.py`** - Comprehensive test suite for all components

### Deployment
- **`deploy_to_ec2.sh`** - Deployment script for EC2 instances

### Configuration
- **`.env.local.example`** - Environment configuration template
- **`.env.local`** - Your actual environment configuration (git-ignored)
- **`requirements.txt`** - Python dependencies (optional, uses stdlib only)

### Documentation
- **`README.md`** - Comprehensive documentation
- **`PROJECT_STRUCTURE.md`** - This file

### Version Control
- **`.gitignore`** - Git ignore configuration

## Directories

### Reference Files
- **`n8n_workflow_jsons/`** - N8N workflow configurations
  - `company_discovery.json`
  - `company_enrichment.json`
  - `contact_discovery_fallback.json`
  - `contact_enrichment.json`
  - `email_discovery_verify.json`

### Generated/Temporary (git-ignored)
- **`runs/`** - Pipeline run artifacts (when created)
- **`logs/`** - Log files (when created)
- **`__pycache__/`** - Python bytecode cache
- **`.pytest_cache/`** - Pytest cache

### Archives
- **`runs_archive.tar.gz`** - Archived historical run data

## File Organization Principles

1. **Minimal Dependencies** - Core functionality uses Python stdlib only
2. **Single Responsibility** - Each file has a clear, focused purpose
3. **Self-Contained** - Main pipeline (`lead_pipeline.py`) contains all core logic
4. **Test Coverage** - All functionality testable via `test_suite.py`
5. **Documentation** - Single comprehensive README for all documentation
6. **Clean Repository** - No temporary files, caches, or redundant code

## Quick Reference

```bash
# Run pipeline
python3 lead_pipeline.py --state TN --quantity 20

# Run tests
python3 test_suite.py

# Process queue
python3 queue_worker.py

# Deploy to EC2
./deploy_to_ec2.sh user@host
```