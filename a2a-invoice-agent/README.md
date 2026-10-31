# A2A Invoice Agent

Work-in-progress implementation of the A2A 1.0 invoice action agent challenge.

## Status

This branch is a development baseline, not a grader-ready submission. Do not submit its URL or spend grader checks until the contract, concurrency, isolation, and receipt lifecycle tests pass.

## Local run

1. Create a virtual environment and install `requirements.txt`.
2. Set `DATABASE_URL`, `A2A_BASE_URL`, `A2A_BEARER_TOKENS`, and `GEMINI_API_KEY` as environment variables.
3. Run `uvicorn main:app --host 0.0.0.0 --port 8000`.

The Gemini key must be stored as a deployment secret and never committed.