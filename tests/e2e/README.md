# End-to-End Tests

End-to-end tests verify complete pipeline execution from start to finish.

## Status

🚧 **Phase 4 - Coming Soon**

E2E tests are planned but not yet implemented. See placeholder tests in `test_pipeline_e2e.py`.

## Planned Tests

### Small Batch Pipeline
- Run complete pipeline with 5 companies
- Verify all stages execute correctly
- Check output files are generated
- Validate metrics and summaries

### Error Recovery
- Test discovery webhook failure recovery
- Test enrichment failure handling
- Test interrupt and resume from checkpoint
- Test partial results on errors

### Quality Gates
- Test contact quality rejection
- Test various filter combinations
- Test edge cases and boundary conditions

## Running E2E Tests

```bash
# Once implemented, run with:
pytest -m e2e

# E2E tests will be slower, so optionally:
pytest -m e2e --timeout=300
```

## Implementation Checklist

- [ ] Set up realistic test data fixtures
- [ ] Mock all external webhooks (n8n, Supabase, HubSpot)
- [ ] Create temporary run directories for each test
- [ ] Test complete pipeline with 5-10 companies
- [ ] Test error scenarios and recovery
- [ ] Test quality gate enforcement
- [ ] Add performance benchmarks

## Contributing

If implementing e2e tests:
1. Remove `@pytest.mark.skip` from test placeholders
2. Add realistic mocks for external services
3. Use `tmp_path` fixtures for run directories
4. Keep tests fast (<30 seconds each)
5. Add detailed assertions on outputs

See `TESTING_GUIDE.md` for test writing guidelines.
