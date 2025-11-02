# Request Splitting Feature Documentation

## Overview
The request splitting feature automatically divides large lead generation requests (>10 companies) into smaller, non-overlapping chunks to improve performance, reliability, and processing efficiency.

## Key Benefits
- **Better Performance**: Smaller chunks process faster and more reliably
- **Fault Tolerance**: If one chunk fails, others can still complete
- **Parallel Processing**: Chunks can potentially run concurrently
- **Resource Management**: Prevents memory/timeout issues with large requests
- **Better Tracking**: Each chunk can be monitored independently

## Architecture

### Components

1. **LLMRequestSplitter** (`request_splitter.py`)
   - Uses LLM (Anthropic Claude or OpenAI GPT) to intelligently split requests
   - Ensures no overlap between chunks using various strategies
   - Falls back to alphabetical splitting if LLM is unavailable

2. **RequestChunkManager** (`request_splitter.py`)
   - Manages creation and tracking of sub-requests
   - Handles parent-child relationships
   - Aggregates results from chunks

3. **EnrichmentRequestProcessor** (`lead_pipeline.py`)
   - Detects large requests (>10 companies)
   - Triggers splitting for eligible requests
   - Creates sub-requests in the database

## How It Works

### 1. Detection
When a request is processed, the system checks:
```python
if quantity > 10 and not parameters.get("is_chunk", False):
    # Split this request
```

### 2. Splitting Strategy
The LLM analyzes the request and chooses an optimal splitting strategy:
- **Geographic**: Split by cities or regions
- **Unit Ranges**: Divide by property size ranges
- **Alphabetical**: Company names A-F, G-L, etc.
- **Property Type**: Multifamily, single-family, etc.
- **Mixed**: Combination of strategies

### 3. Sub-Request Creation
Each chunk becomes a new request with:
- Modified parameters to ensure no overlap
- Reference to parent request ID
- Chunk metadata (index, total chunks, split criteria)

### 4. Processing
Sub-requests are processed independently through the normal pipeline.

### 5. Result Aggregation
Once all chunks complete, results are aggregated back to the parent request.

## Example Scenarios

### Scenario 1: Geographic Split
**Original Request:**
- Quantity: 30
- State: California
- PMS: AppFolio

**Split Into:**
1. Chunk 1: 10 companies in Northern California
2. Chunk 2: 10 companies in Southern California
3. Chunk 3: 10 companies in Central California

### Scenario 2: Unit Range Split
**Original Request:**
- Quantity: 25
- Unit Min: 50
- Unit Max: 500

**Split Into:**
1. Chunk 1: 10 companies with 50-200 units
2. Chunk 2: 10 companies with 201-350 units
3. Chunk 3: 5 companies with 351-500 units

### Scenario 3: Alphabetical Split (Fallback)
**Original Request:**
- Quantity: 20
- City: Memphis
- State: TN

**Split Into:**
1. Chunk 1: 10 companies (names A-M)
2. Chunk 2: 10 companies (names N-Z)

## Configuration

### Environment Variables
```bash
# Required for intelligent splitting (choose one):
export ANTHROPIC_API_KEY="your-anthropic-key"
# OR
export OPENAI_API_KEY="your-openai-key"

# Optional: Override chunk size (default: 10)
export REQUEST_CHUNK_SIZE=10
```

### Installation
```bash
# Install required dependencies
pip install anthropic openai

# Deploy to production
./deploy_to_ec2.sh ec2-user@10.0.131.72
```

## Database Schema

### Parent Request
```json
{
  "id": 1234,
  "workflow_status": "split_into_chunks",
  "request": {
    "parameters": {
      "quantity": 30,
      "state": "CA",
      "pms": "AppFolio"
    }
  },
  "run_logs": {
    "split_time": "2025-10-31T12:00:00Z",
    "num_chunks": 3,
    "sub_request_ids": [1235, 1236, 1237]
  }
}
```

### Sub-Request (Chunk)
```json
{
  "id": 1235,
  "workflow_status": "pending",
  "request": {
    "parameters": {
      "quantity": 10,
      "state": "CA",
      "city": "San Francisco",
      "pms": "AppFolio",
      "is_chunk": true,
      "parent_request_id": 1234,
      "chunk_filters": {
        "filter": "Northern California region"
      }
    },
    "chunk_info": {
      "chunk_id": "1234_chunk_0",
      "chunk_index": 0,
      "total_chunks": 3,
      "split_criteria": "geographic: Northern California"
    }
  }
}
```

## Testing

### Test Without Dependencies
```bash
# Basic integration test
python3 test_request_splitting.py
```

### Test With LLM
```bash
# Set API key
export ANTHROPIC_API_KEY="your-key"

# Run full test
python3 test_request_splitting.py
```

### Manual Test
```bash
# Submit a large request
python3 lead_pipeline.py --state CA --pms AppFolio --quantity 25

# Monitor processing
python3 monitor_queue.py
```

## Monitoring

### Check Split Requests
```sql
-- Find parent requests that were split
SELECT * FROM enrichment_requests
WHERE workflow_status = 'split_into_chunks'
ORDER BY request_time DESC;

-- Find sub-requests for a parent
SELECT * FROM enrichment_requests
WHERE request->'parent_request_id' = '1234';
```

### View Logs
```bash
# Check for splitting activity
ssh -i ~/.ssh/neo_aws ec2-user@10.0.131.72 \
  'sudo journalctl -u lead-list-queue.service | grep -i split'
```

## Troubleshooting

### Issue: Splitting Not Working
**Symptoms:** Large requests processed as single batch
**Solutions:**
1. Check LLM API key is set
2. Verify dependencies installed: `pip install anthropic openai`
3. Check logs for import errors

### Issue: Chunks Overlapping
**Symptoms:** Same companies appearing in multiple chunks
**Solutions:**
1. Review split strategy in logs
2. Ensure chunk filters are being applied
3. Check alphabetical fallback is working

### Issue: Results Not Aggregating
**Symptoms:** Parent request stays in "split_into_chunks" status
**Solutions:**
1. Verify all sub-requests completed
2. Check for failed chunks
3. Manual aggregation may be needed

## Performance Considerations

- **Chunk Size**: Default is 10, adjust based on API limits
- **LLM Latency**: Adds 1-2 seconds for splitting decision
- **Database Load**: Creates N+1 requests (parent + chunks)
- **Processing Time**: Overall faster due to smaller batches

## Future Enhancements

1. **Parallel Chunk Processing**: Run chunks concurrently
2. **Smart Retry**: Re-split failed chunks with different strategy
3. **Dynamic Chunk Sizing**: Adjust based on complexity
4. **Result Streaming**: Return results as chunks complete
5. **ML-Based Strategy**: Learn optimal split strategies from history

## Security Notes

- API keys stored in environment variables only
- No sensitive data sent to LLM (only request parameters)
- Fallback ensures functionality without external dependencies
- Sub-requests inherit parent's authorization context