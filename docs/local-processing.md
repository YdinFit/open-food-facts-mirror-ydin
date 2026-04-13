# Local Processing

Run the `process_chunk` binary locally to process the Open Food Facts CSV and upload products to R2.

## Prerequisites

- Rust toolchain (`rustup` + `cargo`)
- AWS CLI (for verifying R2 uploads)
- R2 credentials (access key + secret key + account ID)

## Build

```bash
cargo build --release
# Binary: ./target/release/process_chunk
```

## Download the CSV

```bash
mkdir -p food_facts_raw_data
curl -L https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz \
  -o food_facts_raw_data/products.csv.gz
```

This is ~1.5 GB compressed (15–20 GB uncompressed). Allow 5–10 minutes.

## Run

```bash
./target/release/process_chunk \
  --input-file food_facts_raw_data/products.csv.gz \
  --skip 0 \
  --take 9999999 \
  --output-dir ./output \
  --r2-endpoint https://<CLOUDFLARE_ACCOUNT_ID>.r2.cloudflarestorage.com \
  --r2-bucket ydin-open-food-facts-prod \
  --r2-access-key <R2_ACCESS_KEY_ID> \
  --r2-secret-key <R2_SECRET_ACCESS_KEY> \
  --r2-concurrency 50
```

### Arguments

| Argument | Description |
|----------|-------------|
| `--input-file` | Path to `products.csv.gz` |
| `--skip N` | Skip first N data rows (0-indexed, after header). Used for chunk parallelism. |
| `--take M` | Process at most M rows. Use `9999999` for "rest of file". |
| `--output-dir` | Directory for partial catalog JSONL files and `changed_codes.txt` |
| `--r2-endpoint` | R2 S3-compatible endpoint URL |
| `--r2-bucket` | R2 bucket name |
| `--r2-access-key` | R2 access key ID |
| `--r2-secret-key` | R2 secret access key |
| `--r2-concurrency` | Max concurrent R2 PUT requests (default: 50) |

## Output

- `{output-dir}/catalogs/{cc}/chunk.jsonl` — partial catalog for each country code (uncompressed JSONL)
- `{output-dir}/changed_codes.txt` — one product barcode per line (all processed products)
- R2: `products/{barcode}.json` — individual product JSON uploaded directly

## Test run (first 1000 products)

```bash
./target/release/process_chunk \
  --skip 0 \
  --take 1000 \
  --output-dir ./test-output \
  --r2-endpoint https://<CLOUDFLARE_ACCOUNT_ID>.r2.cloudflarestorage.com \
  --r2-bucket ydin-open-food-facts-prod \
  --r2-access-key <KEY> \
  --r2-secret-key <SECRET>
```

Verify in R2:
```bash
aws s3 ls s3://ydin-open-food-facts-prod/products/ \
  --endpoint-url https://<CLOUDFLARE_ACCOUNT_ID>.r2.cloudflarestorage.com | head -20
```
