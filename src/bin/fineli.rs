use anyhow::{Context, Result};
use clap::Parser;
use open_food_facts_mirror::{
    setup_s3_client, upload_if_changed, AminoAcids, Breakdown, CatalogEntry, CatalogWriter,
    FatBreakdown, MacroNutrients, Minerals, OtherNutrients, Product, R2Config, Vitamins,
};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

// ---- CLI Args ----
#[derive(Parser, Debug)]
#[command(name = "fineli")]
struct Args {
    /// Path to fineli.zip (downloaded from fineli.fi/fineli/content/file/48)
    #[arg(long, default_value = "fineli_data/fineli.zip")]
    input_file: PathBuf,

    /// Directory for catalog + changed_codes.txt
    #[arg(long, default_value = "output")]
    output_dir: PathBuf,

    /// R2 endpoint URL (e.g. https://<acct>.r2.cloudflarestorage.com)
    #[arg(long)]
    r2_endpoint: String,

    /// R2 bucket name
    #[arg(long)]
    r2_bucket: String,

    /// R2 access key ID
    #[arg(long)]
    r2_access_key: String,

    /// R2 secret access key
    #[arg(long)]
    r2_secret_key: String,

    /// Max concurrent R2 PUTs
    #[arg(long, default_value_t = 50)]
    r2_concurrency: usize,
}

// ---- Fineli CSV Parsing ----

fn parse_fineli_float(s: &str) -> Option<f64> {
    let cleaned = s.trim().replace(',', ".");
    if cleaned.is_empty() {
        return None;
    }
    cleaned.parse().ok().filter(|n: &f64| n.is_finite())
}

struct FineliData {
    /// food_id -> (eufdname -> per-100g value)
    nutrients: HashMap<u32, HashMap<String, f64>>,
    /// food_id -> English name
    names_en: HashMap<u32, String>,
    /// food_id -> Finnish name
    names_fi: HashMap<u32, String>,
}

fn parse_fineli_zip(path: &PathBuf) -> Result<FineliData> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open {:?}", path))?;
    let mut archive = zip::ZipArchive::new(file)
        .context("Failed to read zip archive")?;

    // Parse component_value.csv: FOODID;EUFDNAME;BESTLOC;ACQTYPE;METHTYPE;METHIND
    let mut nutrients: HashMap<u32, HashMap<String, f64>> = HashMap::new();
    {
        let mut zip_file = archive.by_name("component_value.csv")
            .context("component_value.csv not found in zip")?;
        let mut bytes = Vec::new();
        zip_file.read_to_end(&mut bytes)?;
        let content = String::from_utf8_lossy(&bytes);
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(content.as_bytes());
        for result in reader.records() {
            let record = result?;
            let food_id: u32 = match record.get(0).and_then(|s| s.parse().ok()) {
                Some(id) => id,
                None => continue,
            };
            let eufdname = match record.get(1) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => continue,
            };
            let value = match record.get(2).and_then(|s| parse_fineli_float(s)) {
                Some(v) => v,
                None => continue,
            };
            nutrients.entry(food_id).or_default().insert(eufdname, value);
        }
    }

    // Parse foodname_EN.csv: FOODID;FOODNAME;LANG
    let mut names_en: HashMap<u32, String> = HashMap::new();
    {
        let mut zip_file = archive.by_name("foodname_EN.csv")
            .context("foodname_EN.csv not found in zip")?;
        let mut bytes = Vec::new();
        zip_file.read_to_end(&mut bytes)?;
        let content = String::from_utf8_lossy(&bytes);
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(content.as_bytes());
        for result in reader.records() {
            let record = result?;
            let food_id: u32 = match record.get(0).and_then(|s| s.parse().ok()) {
                Some(id) => id,
                None => continue,
            };
            if let Some(name) = record.get(1).filter(|s| !s.is_empty()) {
                names_en.insert(food_id, name.to_string());
            }
        }
    }

    // Parse foodname_FI.csv: FOODID;FOODNAME;LANG
    let mut names_fi: HashMap<u32, String> = HashMap::new();
    {
        let mut zip_file = archive.by_name("foodname_FI.csv")
            .context("foodname_FI.csv not found in zip")?;
        let mut bytes = Vec::new();
        zip_file.read_to_end(&mut bytes)?;
        let content = String::from_utf8_lossy(&bytes);
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(content.as_bytes());
        for result in reader.records() {
            let record = result?;
            let food_id: u32 = match record.get(0).and_then(|s| s.parse().ok()) {
                Some(id) => id,
                None => continue,
            };
            if let Some(name) = record.get(1).filter(|s| !s.is_empty()) {
                names_fi.insert(food_id, name.to_string());
            }
        }
    }

    Ok(FineliData { nutrients, names_en, names_fi })
}

// ---- Product Building ----

fn build_product(
    food_id: u32,
    nutrients: &HashMap<String, f64>,
    names_en: &HashMap<u32, String>,
    names_fi: &HashMap<u32, String>,
) -> Product {
    let get = |key: &str| nutrients.get(key).copied();

    let energy_kj = get("ENERC");
    // Fineli only stores kJ; derive kcal (1 kcal = 4.184 kJ)
    let energy_kcal = energy_kj.map(|kj| kj / 4.184);

    Product {
        code: format!("fineli:{}", food_id),
        source: "fineli".to_string(),
        product_name: names_en.get(&food_id).cloned(),
        generic_name: names_fi.get(&food_id).cloned(),
        ingredients_text: None,
        brands: None,
        main_category: None,
        serving_size: None,
        serving_unit: None,
        breakdown: Breakdown {
            macros: MacroNutrients {
                energy_kcal,
                energy_kj,
                carbohydrates: get("CHOAVL"),
                fat: get("FAT"),
                proteins: get("PROT"),
                sugars: get("SUGAR"),
                fiber: get("FIBC"),
                salt: get("NACL"),
                starch: get("STARCH"),
                added_sugars: None,
                sucrose: get("SUCS"),
                glucose: get("GLUS"),
                fructose: get("FRUS"),
                galactose: get("GALS"),
                lactose: get("LACS"),
                maltose: get("MALS"),
                maltodextrins: None,
                psicose: None,
                polyols: get("PSACNCS"),
                erythritol: None,
                isomalt: None,
                maltitol: None,
                sorbitol: None,
                soluble_fiber: None,
                insoluble_fiber: get("FIBINS"),
                polydextrose: None,
            },
            vitamins: Vitamins {
                vitamin_a: get("VITA"),
                beta_carotene: get("CAROTENS"),
                vitamin_d: get("VITD"),
                vitamin_e: get("VITE"),
                vitamin_k: get("VITK"),
                vitamin_c: get("VITC"),
                vitamin_b1: get("THIA"),
                vitamin_b2: get("RIBF"),
                vitamin_pp: get("NIA"),
                vitamin_b6: get("VITPYRID"),
                vitamin_b9: get("FOL"),
                folates: get("FOL"),
                vitamin_b12: get("VITB12"),
                biotin: None,
                pantothenic_acid: None,
                choline: None,
                phylloquinone: None,
                inositol: None,
            },
            minerals: Minerals {
                sodium: get("NA"),
                calcium: get("CA"),
                phosphorus: get("P"),
                iron: get("FE"),
                magnesium: get("MG"),
                zinc: get("ZN"),
                copper: None,
                manganese: None,
                fluoride: None,
                selenium: get("SE"),
                chromium: None,
                molybdenum: None,
                iodine: get("ID"),
                potassium: get("K"),
                chloride: None,
                silica: None,
                bicarbonate: None,
                sulphate: None,
                nitrate: None,
            },
            fats: FatBreakdown {
                saturated: get("FASAT"),
                unsaturated: None,
                monounsaturated: get("FAMCIS"),
                polyunsaturated: get("FAPU"),
                trans: get("FATRN"),
                cholesterol: get("CHOLE"),
                omega_3: get("FAPUN3"),
                omega_6: get("FAPUN6"),
                omega_9: None,
                alpha_linolenic_acid: get("F18D3N3"),
                eicosapentaenoic_acid: get("F20D5N3"),
                docosahexaenoic_acid: get("F22D6N3"),
                linoleic_acid: get("F18D2CN6"),
                arachidonic_acid: None,
                gamma_linolenic_acid: None,
                dihomo_gamma_linolenic_acid: None,
                oleic_acid: None,
                elaidic_acid: None,
                gondoic_acid: None,
                mead_acid: None,
                erucic_acid: None,
                nervonic_acid: None,
                butyric_acid: None,
                caproic_acid: None,
                caprylic_acid: None,
                capric_acid: None,
                lauric_acid: None,
                myristic_acid: None,
                palmitic_acid: None,
                stearic_acid: None,
                arachidic_acid: None,
                behenic_acid: None,
                lignoceric_acid: None,
                cerotic_acid: None,
                montanic_acid: None,
                melissic_acid: None,
            },
            other: OtherNutrients {
                caffeine: None,
                taurine: None,
                carnitine: None,
                beta_glucan: None,
                alcohol: get("ALC"),
                nucleotides: None,
                casein: None,
                serum_proteins: None,
                methylsulfonylmethane: None,
                energy_from_fat: None,
                added_salt: None,
            },
            amino_acids: AminoAcids {
                // Fineli stores tryptophan in mg; our model uses g
                tryptophan: get("TRP").map(|v| v / 1000.0),
                histidine: None,
                isoleucine: None,
                leucine: None,
                lysine: None,
                methionine: None,
                phenylalanine: None,
                threonine: None,
                valine: None,
                alanine: None,
                arginine: None,
                aspartic_acid: None,
                cysteine: None,
                glutamic_acid: None,
                glycine: None,
                proline: None,
                serine: None,
                tyrosine: None,
                hydroxyproline: None,
                cystine: None,
            },
        },
    }
}

// ---- Main ----
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("fineli starting");
    println!("  input:      {:?}", args.input_file);
    println!("  output_dir: {:?}", args.output_dir);
    println!("  r2_bucket:  {}", args.r2_bucket);
    println!("  concurrency:{}", args.r2_concurrency);

    let r2_config = R2Config {
        endpoint: args.r2_endpoint.clone(),
        bucket: args.r2_bucket.clone(),
        access_key: args.r2_access_key.clone(),
        secret_key: args.r2_secret_key.clone(),
        concurrency: args.r2_concurrency,
    };
    let s3_client = Arc::new(setup_s3_client(&r2_config).await?);
    let semaphore = Arc::new(Semaphore::new(args.r2_concurrency));
    let bucket = Arc::new(args.r2_bucket.clone());

    println!("Parsing Fineli zip...");
    let data = parse_fineli_zip(&args.input_file)?;
    println!("  Found {} foods, {} EN names, {} FI names",
        data.nutrients.len(), data.names_en.len(), data.names_fi.len());

    let catalog_base = args.output_dir.join("catalogs");
    let mut catalog_writer = CatalogWriter::new(catalog_base)?;

    let changed_path = args.output_dir.join("changed_codes.txt");
    let changed_file = File::create(&changed_path)
        .with_context(|| format!("Failed to create {:?}", changed_path))?;
    let mut changed_writer = BufWriter::new(changed_file);

    let mut upload_tasks: JoinSet<Option<String>> = JoinSet::new();

    let mut food_ids: Vec<u32> = data.nutrients.keys().copied().collect();
    food_ids.sort();

    for food_id in &food_ids {
        let food_nutrients = &data.nutrients[food_id];
        let product = build_product(*food_id, food_nutrients, &data.names_en, &data.names_fi);

        let catalog_entry = CatalogEntry {
            code: product.code.clone(),
            name: product.product_name.clone(),
            brand: None,
            country: None,
            serving_size: Some(100.0),
            serving_unit: Some("g".to_string()),
            fiber: food_nutrients.get("FIBC").copied(),
            carbs: food_nutrients.get("CHOAVL").copied(),
            fat: food_nutrients.get("FAT").copied(),
            protein: food_nutrients.get("PROT").copied(),
        };
        catalog_writer.write_entry("fineli", &catalog_entry)?;

        let json_bytes = serde_json::to_vec(&product)
            .with_context(|| format!("Failed to serialize product fineli:{}", food_id))?;
        let code = product.code.clone();
        let key = format!("products/fineli/{}.json", food_id);

        let sem = semaphore.clone();
        let client = s3_client.clone();
        let bkt = bucket.clone();
        upload_tasks.spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            match upload_if_changed(&client, &bkt, &key, json_bytes).await {
                Ok(true) => Some(code),
                Ok(false) => None,
                Err(e) => {
                    eprintln!("Upload error {}: {}", key, e);
                    None
                }
            }
        });
    }

    catalog_writer.flush_all()?;

    println!("Waiting for {} pending uploads...", upload_tasks.len());
    let mut total_uploaded = 0usize;
    let mut total_unchanged = 0usize;
    while let Some(r) = upload_tasks.join_next().await {
        match r {
            Ok(Some(code)) => {
                writeln!(changed_writer, "{}", code)
                    .with_context(|| "Failed to write changed code")?;
                total_uploaded += 1;
            }
            Ok(None) => total_unchanged += 1,
            Err(e) => eprintln!("Upload task panicked: {}", e),
        }
    }
    changed_writer.flush()
        .with_context(|| "Failed to flush changed_codes.txt")?;

    println!("\nDone:");
    println!("  Foods:     {}", food_ids.len());
    println!("  Uploaded:  {}", total_uploaded);
    println!("  Unchanged: {}", total_unchanged);

    Ok(())
}
