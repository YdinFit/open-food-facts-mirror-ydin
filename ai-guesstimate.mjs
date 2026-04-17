#!/usr/bin/env node
/**
 * AI micronutrient estimator for Open Food Facts products.
 *
 * Usage (CI / GitHub Models):
 *   node ai-guesstimate.mjs --limit 150
 *
 * Usage (local Ollama bulk seed):
 *   node ai-guesstimate.mjs --model ollama --codes-file ./codes.txt --concurrency 10
 *
 * Downloads the AI pending queue from R2, estimates missing micronutrients using
 * GPT-4o Mini, and uploads enriched product JSONs back to R2.
 */

import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import OpenAI from 'openai';
import fs from 'node:fs';

// ---- CLI Args ----
function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    model: process.env.GITHUB_ACTIONS === 'true' ? 'github' : 'ollama',
    limit: Infinity,
    concurrency: process.env.GITHUB_ACTIONS === 'true' ? 3 : 10,
    codesFile: null,
    progressFile: 'ai-progress.json',
    enableSearch: false,
  };
  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--model':        opts.model = args[++i]; break;
      case '--limit':        opts.limit = parseInt(args[++i], 10); break;
      case '--concurrency':  opts.concurrency = parseInt(args[++i], 10); break;
      case '--codes-file':   opts.codesFile = args[++i]; break;
      case '--progress-file': opts.progressFile = args[++i]; break;
      case '--enable-search': opts.enableSearch = true; break;
      default:
        console.error(`Unknown arg: ${args[i]}`);
        process.exit(1);
    }
  }
  return opts;
}

// ---- Model Endpoints ----
const ENDPOINTS = {
  github: {
    baseURL: 'https://models.inference.ai.azure.com',
    apiKey: process.env.GITHUB_TOKEN,
    model: 'gpt-4o-mini',
  },
  ollama: {
    baseURL: 'http://localhost:11434/v1',
    apiKey: 'ollama',
    model: 'qwen2.5:14b',
  },
};

// ---- R2 Client ----
function makeR2Client() {
  const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
  if (!accountId) throw new Error('CLOUDFLARE_ACCOUNT_ID not set');
  return new S3Client({
    region: 'auto',
    endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });
}

const BUCKET = 'ydin-open-food-facts-prod';
const AI_PENDING_KEY = 'meta/ai_pending.txt';

async function r2Get(client, key) {
  const res = await client.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
  const chunks = [];
  for await (const chunk of res.Body) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf8');
}

async function r2Put(client, key, body, contentType = 'application/json') {
  await client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    Body: typeof body === 'string' ? Buffer.from(body) : body,
    ContentType: contentType,
  }));
}

// ---- Progress Tracking ----
function loadProgress(progressFile) {
  if (fs.existsSync(progressFile)) {
    return JSON.parse(fs.readFileSync(progressFile, 'utf8'));
  }
  return { processed: [], skipped: [], failed: [] };
}

function saveProgress(progressFile, progress) {
  fs.writeFileSync(progressFile, JSON.stringify(progress, null, 2));
}

// ---- Prompt Builder ----
function buildPrompt(product) {
  const macros = product.breakdown?.macros ?? {};
  const existing = {
    vitamins: product.breakdown?.vitamins ?? {},
    minerals: product.breakdown?.minerals ?? {},
    amino_acids: product.breakdown?.amino_acids ?? {},
  };

  const VITAMIN_FIELDS = [
    'vitamin_a', 'beta_carotene', 'vitamin_d', 'vitamin_e', 'vitamin_k',
    'vitamin_c', 'vitamin_b1', 'vitamin_b2', 'vitamin_pp', 'vitamin_b6',
    'vitamin_b9', 'folates', 'vitamin_b12', 'biotin', 'pantothenic_acid',
    'choline', 'phylloquinone', 'inositol',
  ];
  const MINERAL_FIELDS = [
    'sodium', 'calcium', 'phosphorus', 'iron', 'magnesium', 'zinc', 'copper',
    'manganese', 'fluoride', 'selenium', 'chromium', 'molybdenum', 'iodine',
    'potassium', 'chloride', 'silica', 'bicarbonate', 'sulphate', 'nitrate',
  ];
  const AMINO_FIELDS = [
    'histidine', 'isoleucine', 'leucine', 'lysine', 'methionine', 'phenylalanine',
    'threonine', 'tryptophan', 'valine', 'alanine', 'arginine', 'aspartic_acid',
    'cysteine', 'glutamic_acid', 'glycine', 'proline', 'serine', 'tyrosine',
    'hydroxyproline', 'cystine',
  ];

  const missing = {
    vitamins: VITAMIN_FIELDS.filter(f => existing.vitamins[f] == null),
    minerals: MINERAL_FIELDS.filter(f => existing.minerals[f] == null),
    amino_acids: AMINO_FIELDS.filter(f => existing.amino_acids[f] == null),
  };

  const allMissing = [
    ...missing.vitamins,
    ...missing.minerals,
    ...missing.amino_acids,
  ];

  if (allMissing.length === 0) {
    return null; // Nothing to estimate
  }

  const summary = {
    product_name: product.product_name,
    brands: product.brands,
    ingredients_text: product.ingredients_text?.slice(0, 500),
    macros: {
      energy_kcal: macros.energy_kcal,
      fat: macros.fat,
      proteins: macros.proteins,
      carbohydrates: macros.carbohydrates,
    },
    missing_fields: allMissing,
  };

  return { summary };
}

// ---- AI Estimation ----
async function estimateProduct(openaiClient, product, opts) {
  const built = buildPrompt(product);
  if (!built) return { action: 'skipped', reason: 'nothing_missing' };

  const { summary } = built;

  const systemPrompt = `You are a nutrition database AI. Estimate missing nutritional values for a food product based on its ingredients and known macronutrients. Respond ONLY with a valid JSON object. No markdown, no explanations, no text outside the JSON.`;

  const userPrompt = `Estimate missing micronutrients (per 100g) for this product.

Return exactly this structure — only include the fields listed in "missing_fields":
{
  "action": "estimated",
  "vitamins":    { "field_name": number_or_null, ... },
  "minerals":    { "field_name": number_or_null, ... },
  "amino_acids": { "field_name": number_or_null, ... }
}

Set action to "skipped" only if the product is completely unidentifiable.
Units: vitamin_a(µg RAE), vitamin_d(µg), vitamin_e(mg), vitamin_c(mg),
       vitamin_b1/b2/b6/pp(mg), vitamin_b9/b12/biotin(µg),
       minerals mostly mg except selenium/iodine/chromium/molybdenum(µg),
       amino acids: g per 100g.

Example product input:
{"product_name":"Nutella","ingredients_text":"sugar, palm oil, hazelnuts 13%, cocoa, skimmed milk powder...","macros":{"energy_kcal":539,"fat":30.9,"proteins":6.3,"carbohydrates":57.5},"missing_fields":["calcium","iron","vitamin_e"]}

Example response:
{"action":"estimated","vitamins":{"vitamin_e":1.2},"minerals":{"calcium":95,"iron":2.5},"amino_acids":{}}

Product:
${JSON.stringify(summary)}`;

  const tools = opts.enableSearch
    ? [{
        type: 'function',
        function: {
          name: 'search_product',
          description: 'Search for a food product by barcode or name to find its ingredients and nutritional info',
          parameters: {
            type: 'object',
            properties: { query: { type: 'string' } },
            required: ['query'],
          },
        },
      }]
    : undefined;

  const messages = [
    { role: 'system', content: systemPrompt },
    { role: 'user', content: userPrompt },
  ];

  // Tool call loop (for search tool support)
  for (let turn = 0; turn < 5; turn++) {
    const response = await openaiClient.chat.completions.create({
      model: ENDPOINTS[opts.model].model,
      messages,
      response_format: {
        type: 'json_schema',
        json_schema: {
          name: 'nutrition_estimate',
          strict: true,
          schema: {
            type: 'object',
            properties: {
              action: { type: 'string', enum: ['estimated', 'skipped'] },
              vitamins: {
                type: 'object',
                properties: {
                  vitamin_a:        { type: ['number', 'null'] },
                  beta_carotene:    { type: ['number', 'null'] },
                  vitamin_d:        { type: ['number', 'null'] },
                  vitamin_e:        { type: ['number', 'null'] },
                  vitamin_k:        { type: ['number', 'null'] },
                  vitamin_c:        { type: ['number', 'null'] },
                  vitamin_b1:       { type: ['number', 'null'] },
                  vitamin_b2:       { type: ['number', 'null'] },
                  vitamin_pp:       { type: ['number', 'null'] },
                  vitamin_b6:       { type: ['number', 'null'] },
                  vitamin_b9:       { type: ['number', 'null'] },
                  folates:          { type: ['number', 'null'] },
                  vitamin_b12:      { type: ['number', 'null'] },
                  biotin:           { type: ['number', 'null'] },
                  pantothenic_acid: { type: ['number', 'null'] },
                  choline:          { type: ['number', 'null'] },
                  phylloquinone:    { type: ['number', 'null'] },
                  inositol:         { type: ['number', 'null'] },
                },
                required: ['vitamin_a','beta_carotene','vitamin_d','vitamin_e','vitamin_k','vitamin_c','vitamin_b1','vitamin_b2','vitamin_pp','vitamin_b6','vitamin_b9','folates','vitamin_b12','biotin','pantothenic_acid','choline','phylloquinone','inositol'],
                additionalProperties: false,
              },
              minerals: {
                type: 'object',
                properties: {
                  sodium:      { type: ['number', 'null'] },
                  calcium:     { type: ['number', 'null'] },
                  phosphorus:  { type: ['number', 'null'] },
                  iron:        { type: ['number', 'null'] },
                  magnesium:   { type: ['number', 'null'] },
                  zinc:        { type: ['number', 'null'] },
                  copper:      { type: ['number', 'null'] },
                  manganese:   { type: ['number', 'null'] },
                  fluoride:    { type: ['number', 'null'] },
                  selenium:    { type: ['number', 'null'] },
                  chromium:    { type: ['number', 'null'] },
                  molybdenum:  { type: ['number', 'null'] },
                  iodine:      { type: ['number', 'null'] },
                  potassium:   { type: ['number', 'null'] },
                  chloride:    { type: ['number', 'null'] },
                  silica:      { type: ['number', 'null'] },
                  bicarbonate: { type: ['number', 'null'] },
                  sulphate:    { type: ['number', 'null'] },
                  nitrate:     { type: ['number', 'null'] },
                },
                required: ['sodium','calcium','phosphorus','iron','magnesium','zinc','copper','manganese','fluoride','selenium','chromium','molybdenum','iodine','potassium','chloride','silica','bicarbonate','sulphate','nitrate'],
                additionalProperties: false,
              },
              amino_acids: {
                type: 'object',
                properties: {
                  histidine:      { type: ['number', 'null'] },
                  isoleucine:     { type: ['number', 'null'] },
                  leucine:        { type: ['number', 'null'] },
                  lysine:         { type: ['number', 'null'] },
                  methionine:     { type: ['number', 'null'] },
                  phenylalanine:  { type: ['number', 'null'] },
                  threonine:      { type: ['number', 'null'] },
                  tryptophan:     { type: ['number', 'null'] },
                  valine:         { type: ['number', 'null'] },
                  alanine:        { type: ['number', 'null'] },
                  arginine:       { type: ['number', 'null'] },
                  aspartic_acid:  { type: ['number', 'null'] },
                  cysteine:       { type: ['number', 'null'] },
                  glutamic_acid:  { type: ['number', 'null'] },
                  glycine:        { type: ['number', 'null'] },
                  proline:        { type: ['number', 'null'] },
                  serine:         { type: ['number', 'null'] },
                  tyrosine:       { type: ['number', 'null'] },
                  hydroxyproline: { type: ['number', 'null'] },
                  cystine:        { type: ['number', 'null'] },
                },
                required: ['histidine','isoleucine','leucine','lysine','methionine','phenylalanine','threonine','tryptophan','valine','alanine','arginine','aspartic_acid','cysteine','glutamic_acid','glycine','proline','serine','tyrosine','hydroxyproline','cystine'],
                additionalProperties: false,
              },
            },
            required: ['action', 'vitamins', 'minerals', 'amino_acids'],
            additionalProperties: false,
          },
        },
      },
      temperature: 0,
      ...(tools ? { tools, tool_choice: 'auto' } : {}),
    });

    const choice = response.choices[0];

    if (choice.finish_reason === 'tool_calls' && choice.message.tool_calls?.length > 0) {
      messages.push(choice.message);
      for (const tc of choice.message.tool_calls) {
        if (tc.function.name === 'search_product') {
          const { query } = JSON.parse(tc.function.arguments);
          const searchResult = await performSearch(query);
          messages.push({
            role: 'tool',
            tool_call_id: tc.id,
            content: searchResult,
          });
        }
      }
      continue; // Next turn with tool results
    }

    // Parse JSON response
    const content = choice.message.content?.trim() ?? '{}';
    try {
      return JSON.parse(content);
    } catch {
      return { action: 'failed', reason: 'invalid_json', raw: content.slice(0, 200) };
    }
  }

  return { action: 'failed', reason: 'max_turns_exceeded' };
}

async function performSearch(query) {
  const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
  try {
    const res = await fetch(url, {
      headers: { 'Accept': 'application/json' },
    });
    const data = await res.json();
    const parts = [];
    if (data.AbstractText) parts.push(data.AbstractText);
    for (const r of (data.RelatedTopics ?? []).slice(0, 5)) {
      if (r.Text) parts.push(r.Text);
    }
    return parts.join('\n') || 'No results found';
  } catch (e) {
    return `Search error: ${e.message}`;
  }
}

// ---- Apply Estimates ----
function applyEstimates(product, estimates) {
  if (!estimates || estimates.action === 'skipped' || estimates.action === 'failed') {
    return product;
  }

  const breakdown = product.breakdown ?? {};

  if (estimates.vitamins && Object.keys(estimates.vitamins).length > 0) {
    breakdown.vitamins = { ...(breakdown.vitamins ?? {}), ...filterNulls(estimates.vitamins) };
  }
  if (estimates.minerals && Object.keys(estimates.minerals).length > 0) {
    breakdown.minerals = { ...(breakdown.minerals ?? {}), ...filterNulls(estimates.minerals) };
  }
  if (estimates.amino_acids && Object.keys(estimates.amino_acids).length > 0) {
    breakdown.amino_acids = { ...(breakdown.amino_acids ?? {}), ...filterNulls(estimates.amino_acids) };
  }

  return {
    ...product,
    breakdown,
    ai_guesses: {
      model: ENDPOINTS[process.env.GITHUB_ACTIONS === 'true' ? 'github' : 'ollama'].model,
      timestamp: new Date().toISOString(),
    },
  };
}

function filterNulls(obj) {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v != null));
}

// ---- Concurrency Limiter ----
async function withConcurrency(items, concurrency, fn, isStopped = () => false) {
  const results = new Array(items.length);
  let idx = 0;

  async function worker() {
    while (idx < items.length && !isStopped()) {
      const i = idx++;
      results[i] = await fn(items[i], i);
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker));
  return results;
}

// ---- Main ----
async function main() {
  const opts = parseArgs();

  console.log(`AI estimator starting`);
  console.log(`  model:       ${opts.model}`);
  console.log(`  limit:       ${opts.limit === Infinity ? 'unlimited' : opts.limit}`);
  console.log(`  concurrency: ${opts.concurrency}`);
  console.log(`  codesFile:   ${opts.codesFile ?? '(from R2 ai_pending.txt)'}`);
  console.log(`  search:      ${opts.enableSearch}`);

  const endpoint = ENDPOINTS[opts.model];
  if (!endpoint) {
    console.error(`Unknown model: ${opts.model}. Use 'github' or 'ollama'.`);
    process.exit(1);
  }
  if (!endpoint.apiKey) {
    console.error(`API key not set for model '${opts.model}'. Set GITHUB_TOKEN or ensure Ollama is running.`);
    process.exit(1);
  }

  const openaiClient = new OpenAI({ baseURL: endpoint.baseURL, apiKey: endpoint.apiKey });
  const r2 = makeR2Client();

  // Load codes to process
  let codes;
  let sourceIsPending = false;

  if (opts.codesFile) {
    codes = fs.readFileSync(opts.codesFile, 'utf8').split('\n').map(l => l.trim()).filter(Boolean);
    console.log(`Loaded ${codes.length} codes from ${opts.codesFile}`);
  } else {
    // CI mode: read from R2 ai_pending.txt
    let pending;
    try {
      pending = await r2Get(r2, AI_PENDING_KEY);
    } catch (e) {
      if (e.name === 'NoSuchKey' || e.$metadata?.httpStatusCode === 404) {
        console.log('ai_pending.txt not found in R2 — nothing to do');
        process.exit(0);
      }
      throw e;
    }
    codes = pending.split('\n').map(l => l.trim()).filter(Boolean);
    sourceIsPending = true;
    console.log(`Loaded ${codes.length} pending codes from R2`);
  }

  if (codes.length === 0) {
    console.log('No codes to process — exiting');
    process.exit(0);
  }

  // Load progress (local file, only meaningful for local bulk runs)
  const progress = loadProgress(opts.progressFile);
  const alreadyDone = new Set([...progress.processed, ...progress.skipped]);

  // Apply limit and filter already-done
  const toProcess = codes.filter(c => !alreadyDone.has(c)).slice(0, opts.limit);
  console.log(`Processing ${toProcess.length} codes (${alreadyDone.size} already done)`);

  if (toProcess.length === 0) {
    console.log('All codes already processed — exiting');
    process.exit(0);
  }

  let doneCount = 0;
  let failCount = 0;
  let skipCount = 0;
  let rateLimitHit = false;
  const processedThisRun = [];

  await withConcurrency(toProcess, opts.concurrency, async (code) => {
    if (rateLimitHit) return;
    const productKey = `products/${code}.json`;

    // Download product from R2
    let product;
    try {
      const raw = await r2Get(r2, productKey);
      product = JSON.parse(raw);
    } catch (e) {
      console.error(`[${code}] Download failed: ${e.message}`);
      progress.failed.push(code);
      failCount++;
      return;
    }

    // Skip if already estimated
    if (product.ai_guesses) {
      progress.skipped.push(code);
      processedThisRun.push(code);
      skipCount++;
      return;
    }

    // Estimate
    let estimates;
    try {
      estimates = await estimateProduct(openaiClient, product, opts);
    } catch (e) {
      if (e.status === 429) {
        console.log(`[${code}] Daily rate limit reached — stopping. Remaining codes stay in pending queue.`);
        rateLimitHit = true;
        return;
      }
      console.error(`[${code}] Estimation failed: ${e.message}`);
      progress.failed.push(code);
      failCount++;
      return;
    }

    if (estimates.action === 'skipped') {
      console.log(`[${code}] Skipped: ${estimates.reason ?? 'model skipped'}`);
      progress.skipped.push(code);
      processedThisRun.push(code);
      skipCount++;
      return;
    }

    if (estimates.action === 'failed') {
      console.error(`[${code}] Failed: ${estimates.reason}`);
      progress.failed.push(code);
      failCount++;
      return;
    }

    // Apply and upload
    const enriched = applyEstimates(product, estimates);
    try {
      await r2Put(r2, productKey, JSON.stringify(enriched));
    } catch (e) {
      console.error(`[${code}] Upload failed: ${e.message}`);
      progress.failed.push(code);
      failCount++;
      return;
    }

    progress.processed.push(code);
    processedThisRun.push(code);
    doneCount++;
    console.log(`[${code}] Estimated and uploaded (total: ${doneCount + skipCount}/${toProcess.length})`);

    // Save progress periodically (every 50)
    if ((doneCount + skipCount) % 50 === 0) {
      saveProgress(opts.progressFile, progress);
    }
  }, () => rateLimitHit);

  // Final progress save
  saveProgress(opts.progressFile, progress);

  if (rateLimitHit) {
    console.log(`\nStopped at daily rate limit: ${doneCount} estimated, ${skipCount} skipped, ${failCount} failed`);
  } else {
    console.log(`\nDone: ${doneCount} estimated, ${skipCount} skipped, ${failCount} failed`);
  }

  // CI: update ai_pending.txt by removing processed codes
  if (sourceIsPending && processedThisRun.length > 0) {
    const processed = new Set(processedThisRun);
    const remaining = codes.filter(c => !processed.has(c));
    await r2Put(r2, AI_PENDING_KEY, remaining.join('\n') + (remaining.length > 0 ? '\n' : ''), 'text/plain');
    console.log(`Updated ai_pending.txt: ${remaining.length} codes remaining`);
  }
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
