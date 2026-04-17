# Local AI Estimation (Ollama Bulk Seed)

The CI workflow estimates 150 products per day via GitHub Models using `gpt-4o-mini`. Since `gpt-4o-mini` is a proprietary model and cannot be run locally, the bulk seed uses Ollama with `qwen2.5:14b` as an open-weight alternative. For the initial bulk seed of all ~2M products, run this locally using Ollama with parallel model instances.

Estimated throughput: ~20K–35K products overnight (8h) on M3 Max; ~3K–8K on M2 Pro.
Estimated total time for 2M products: **2–3 weeks** on M3 Max, **8–12 weeks** on M2 Pro, running continuously.

## Prerequisites

- Node.js 20+
- [Ollama](https://ollama.ai) installed
- R2 credentials
- `meta/latest_changed_codes.txt` in R2 (produced by the first sync run)

## Setup

### 1. Install Ollama and pull the model

```bash
brew install ollama
ollama pull qwen2.5:14b   # ~9 GB download
```

### 2. Start Ollama with parallel request support

```bash
# M2 Pro (16GB RAM): use 2 parallel (model is ~9GB, leaves little headroom)
OLLAMA_NUM_PARALLEL=2 ollama serve

# M3 Max (64GB RAM): use 5 parallel
OLLAMA_NUM_PARALLEL=5 ollama serve
```

Leave this running in a separate terminal or as a background service.

### 3. Install Node dependencies

```bash
npm install
```

### 4. Download the codes file from R2

After the first sync workflow completes, download the list of all processed product codes:

```bash
aws s3 cp s3://ydin-open-food-facts-prod/meta/latest_changed_codes.txt ./codes.txt \
  --endpoint-url https://<CLOUDFLARE_ACCOUNT_ID>.r2.cloudflarestorage.com
```

## Test run (50 products)

```bash
export CLOUDFLARE_ACCOUNT_ID=<your_account_id>
export AWS_ACCESS_KEY_ID=<r2_access_key>
export AWS_SECRET_ACCESS_KEY=<r2_secret_key>

node ai-guesstimate.mjs \
  --model ollama \
  --codes-file ./codes.txt \
  --concurrency 50 \
  --limit 50
```

Verify a product was enriched:
```bash
aws s3 cp s3://ydin-open-food-facts-prod/products/<some_code>.json - \
  --endpoint-url https://<CLOUDFLARE_ACCOUNT_ID>.r2.cloudflarestorage.com | jq .ai_guesses
```

## Full bulk run

```bash
# Run in tmux or screen so it persists if your terminal closes
tmux new-session -d -s bulk-ai

tmux send-keys -t bulk-ai "
export CLOUDFLARE_ACCOUNT_ID=<your_account_id>
export AWS_ACCESS_KEY_ID=<r2_access_key>
export AWS_SECRET_ACCESS_KEY=<r2_secret_key>

node ai-guesstimate.mjs \
  --model ollama \
  --codes-file ./codes.txt \
  --concurrency 50
" Enter
```

## Resumability

The script writes `ai-progress.json` locally tracking processed/skipped/failed codes. If it's interrupted, restart the same command — it will skip already-processed codes and pick up where it left off.

```bash
# Check progress
node -e "
const p = JSON.parse(require('fs').readFileSync('ai-progress.json'));
console.log('Processed:', p.processed.length);
console.log('Skipped:', p.skipped.length);
console.log('Failed:', p.failed.length);
"
```

## Arguments

| Argument | Description |
|----------|-------------|
| `--model ollama\|github` | Which model endpoint to use (default: ollama locally, github in CI) |
| `--codes-file <path>` | File with one product code per line |
| `--concurrency N` | Parallel requests queued to Ollama (default: 50 locally, 3 in CI) |
| `--limit N` | Stop after N products (useful for testing) |
| `--progress-file <path>` | Progress file path (default: `ai-progress.json`) |
| `--enable-search` | Enable Brave web search tool (requires `BRAVE_API_KEY`) |
