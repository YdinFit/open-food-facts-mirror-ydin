# open-food-facts-mirror

A GitHub Actions pipeline that mirrors the [Open Food Facts](https://world.openfoodfacts.org/) database to Cloudflare R2, with AI-estimated micronutrients.

## What it does

1. **Weekly sync** (`sync.yml`): Downloads the OFF CSV (~1.5 GB compressed), splits it into N parallel chunks, uploads ~2M product JSONs to R2, merges per-country catalog indexes, and queues changed products for AI estimation.

2. **Daily AI estimation** (`ai-estimate.yml`): Takes up to 150 products from the AI queue, estimates missing micronutrients using `gpt-4o-mini` via GitHub Models (free tier), and uploads enriched product JSONs back to R2.

## Architecture

```
R2 bucket: ydin-open-food-facts-prod
Public domain: catalog.ydin.app

products/{barcode}.json          — individual product data
indexes/catalogs/{cc}/catalog.jsonl.br — per-country catalog (brotli JSONL)
meta/ai_pending.txt              — queue of codes awaiting AI estimation
meta/latest_changed_codes.txt    — codes processed in the most recent sync run
```

### Pipeline (sync.yml)

```
trigger: weekly (Sunday 2am UTC) | workflow_dispatch(n_chunks)

[build]        cargo build --release → artifact
[download-csv] curl OFF CSV → artifact
[prepare]      generate chunk matrix [{skip, take}, ...]
[process ×N]   ./process_chunk --skip --take → upload products to R2, write partial catalogs
[merge]        cat partial catalogs | brotli → R2; append changed codes to ai_pending.txt
```

### AI estimation (ai-estimate.yml)

```
trigger: daily (10am UTC) | workflow_dispatch

[estimate] download ai_pending.txt → take 150 → estimate each → upload → remove from queue
```

### Local bulk AI seed (Ollama)

After the first sync run, use Ollama locally to pre-fill all 2M products. See [docs/local-ai-estimation.md](docs/local-ai-estimation.md).

## Required Secrets

| Secret | Description |
|--------|-------------|
| `CLOUDFLARE_ACCOUNT_ID` | Your Cloudflare account ID |
| `R2_ACCESS_KEY_ID` | R2 access key |
| `R2_SECRET_ACCESS_KEY` | R2 secret key |
`GITHUB_TOKEN` is provided automatically by GitHub Actions.

## Local development

- [Running the Rust processor locally](docs/local-processing.md)
- [Running AI estimation locally with Ollama](docs/local-ai-estimation.md)
