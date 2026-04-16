use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{Builder as S3Builder, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub use aws_sdk_s3::Client;

// ---- Data Structures ----

#[derive(Debug, Serialize, Deserialize)]
pub struct Product {
    pub code: String,
    pub source: String,
    pub product_name: Option<String>,
    pub generic_name: Option<String>,
    pub ingredients_text: Option<String>,
    pub brands: Option<String>,
    pub main_category: Option<String>,
    pub serving_size: Option<f64>,
    pub serving_unit: Option<String>,
    pub breakdown: Breakdown,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Breakdown {
    pub macros: MacroNutrients,
    pub vitamins: Vitamins,
    pub minerals: Minerals,
    pub fats: FatBreakdown,
    pub other: OtherNutrients,
    #[serde(default)]
    pub amino_acids: AminoAcids,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MacroNutrients {
    pub energy_kcal: Option<f64>,
    pub energy_kj: Option<f64>,
    pub carbohydrates: Option<f64>,
    pub fat: Option<f64>,
    pub proteins: Option<f64>,
    pub sugars: Option<f64>,
    pub fiber: Option<f64>,
    pub salt: Option<f64>,
    pub added_sugars: Option<f64>,
    pub sucrose: Option<f64>,
    pub glucose: Option<f64>,
    pub fructose: Option<f64>,
    pub galactose: Option<f64>,
    pub lactose: Option<f64>,
    pub maltose: Option<f64>,
    pub maltodextrins: Option<f64>,
    pub psicose: Option<f64>,
    pub starch: Option<f64>,
    pub polyols: Option<f64>,
    pub erythritol: Option<f64>,
    pub isomalt: Option<f64>,
    pub maltitol: Option<f64>,
    pub sorbitol: Option<f64>,
    pub soluble_fiber: Option<f64>,
    pub insoluble_fiber: Option<f64>,
    pub polydextrose: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Vitamins {
    pub vitamin_a: Option<f64>,
    pub beta_carotene: Option<f64>,
    pub vitamin_d: Option<f64>,
    pub vitamin_e: Option<f64>,
    pub vitamin_k: Option<f64>,
    pub vitamin_c: Option<f64>,
    pub vitamin_b1: Option<f64>,
    pub vitamin_b2: Option<f64>,
    pub vitamin_pp: Option<f64>,
    pub vitamin_b6: Option<f64>,
    pub vitamin_b9: Option<f64>,
    pub folates: Option<f64>,
    pub vitamin_b12: Option<f64>,
    pub biotin: Option<f64>,
    pub pantothenic_acid: Option<f64>,
    pub choline: Option<f64>,
    pub phylloquinone: Option<f64>,
    pub inositol: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Minerals {
    pub sodium: Option<f64>,
    pub calcium: Option<f64>,
    pub phosphorus: Option<f64>,
    pub iron: Option<f64>,
    pub magnesium: Option<f64>,
    pub zinc: Option<f64>,
    pub copper: Option<f64>,
    pub manganese: Option<f64>,
    pub fluoride: Option<f64>,
    pub selenium: Option<f64>,
    pub chromium: Option<f64>,
    pub molybdenum: Option<f64>,
    pub iodine: Option<f64>,
    pub potassium: Option<f64>,
    pub chloride: Option<f64>,
    pub silica: Option<f64>,
    pub bicarbonate: Option<f64>,
    pub sulphate: Option<f64>,
    pub nitrate: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FatBreakdown {
    pub saturated: Option<f64>,
    pub unsaturated: Option<f64>,
    pub monounsaturated: Option<f64>,
    pub polyunsaturated: Option<f64>,
    pub trans: Option<f64>,
    pub cholesterol: Option<f64>,
    pub omega_3: Option<f64>,
    pub omega_6: Option<f64>,
    pub omega_9: Option<f64>,
    pub alpha_linolenic_acid: Option<f64>,
    pub eicosapentaenoic_acid: Option<f64>,
    pub docosahexaenoic_acid: Option<f64>,
    pub linoleic_acid: Option<f64>,
    pub arachidonic_acid: Option<f64>,
    pub gamma_linolenic_acid: Option<f64>,
    pub dihomo_gamma_linolenic_acid: Option<f64>,
    pub oleic_acid: Option<f64>,
    pub elaidic_acid: Option<f64>,
    pub gondoic_acid: Option<f64>,
    pub mead_acid: Option<f64>,
    pub erucic_acid: Option<f64>,
    pub nervonic_acid: Option<f64>,
    pub butyric_acid: Option<f64>,
    pub caproic_acid: Option<f64>,
    pub caprylic_acid: Option<f64>,
    pub capric_acid: Option<f64>,
    pub lauric_acid: Option<f64>,
    pub myristic_acid: Option<f64>,
    pub palmitic_acid: Option<f64>,
    pub stearic_acid: Option<f64>,
    pub arachidic_acid: Option<f64>,
    pub behenic_acid: Option<f64>,
    pub lignoceric_acid: Option<f64>,
    pub cerotic_acid: Option<f64>,
    pub montanic_acid: Option<f64>,
    pub melissic_acid: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OtherNutrients {
    pub caffeine: Option<f64>,
    pub taurine: Option<f64>,
    pub carnitine: Option<f64>,
    pub beta_glucan: Option<f64>,
    pub alcohol: Option<f64>,
    pub nucleotides: Option<f64>,
    pub casein: Option<f64>,
    pub serum_proteins: Option<f64>,
    pub methylsulfonylmethane: Option<f64>,
    pub energy_from_fat: Option<f64>,
    pub added_salt: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AminoAcids {
    pub histidine: Option<f64>,
    pub isoleucine: Option<f64>,
    pub leucine: Option<f64>,
    pub lysine: Option<f64>,
    pub methionine: Option<f64>,
    pub phenylalanine: Option<f64>,
    pub threonine: Option<f64>,
    pub tryptophan: Option<f64>,
    pub valine: Option<f64>,
    pub alanine: Option<f64>,
    pub arginine: Option<f64>,
    pub aspartic_acid: Option<f64>,
    pub cysteine: Option<f64>,
    pub glutamic_acid: Option<f64>,
    pub glycine: Option<f64>,
    pub proline: Option<f64>,
    pub serine: Option<f64>,
    pub tyrosine: Option<f64>,
    pub hydroxyproline: Option<f64>,
    pub cystine: Option<f64>,
}

#[derive(Debug)]
pub struct CatalogEntry {
    pub code: String,
    pub name: Option<String>,
    pub brand: Option<String>,
    pub country: Option<String>,
    pub serving_size: Option<f64>,
    pub serving_unit: Option<String>,
    pub fiber: Option<f64>,
    pub carbs: Option<f64>,
    pub fat: Option<f64>,
    pub protein: Option<f64>,
}

impl serde::Serialize for CatalogEntry {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(10))?;
        seq.serialize_element(&self.code)?;
        seq.serialize_element(&self.name)?;
        seq.serialize_element(&self.brand)?;
        seq.serialize_element(&self.country)?;
        seq.serialize_element(&self.serving_size)?;
        seq.serialize_element(&self.serving_unit)?;
        seq.serialize_element(&self.fiber)?;
        seq.serialize_element(&self.carbs)?;
        seq.serialize_element(&self.fat)?;
        seq.serialize_element(&self.protein)?;
        seq.end()
    }
}

// ---- R2 ----

pub struct R2Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub concurrency: usize,
}

pub async fn setup_s3_client(config: &R2Config) -> Result<S3Client> {
    let creds = Credentials::new(
        &config.access_key,
        &config.secret_key,
        None,
        None,
        "cli-args",
    );
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new("auto"))
        .endpoint_url(&config.endpoint)
        .load()
        .await;
    let s3_config = S3Builder::from(&aws_config).force_path_style(true).build();
    Ok(S3Client::from_conf(s3_config))
}

/// Uploads `body` to R2 only if the content has changed (ETag comparison).
/// Returns `true` if uploaded, `false` if skipped (unchanged).
pub async fn upload_if_changed(
    client: &S3Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<bool> {
    let new_etag = format!("{:x}", md5::compute(&body));

    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(head) => {
            let existing = head.e_tag().unwrap_or("").trim_matches('"');
            if existing == new_etag {
                return Ok(false);
            }
        }
        Err(SdkError::ServiceError(e)) if e.raw().status().as_u16() == 404 => {}
        Err(e) => return Err(anyhow::anyhow!("HEAD failed for {}: {}", key, e)),
    }

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .content_type("application/json")
        .send()
        .await
        .with_context(|| format!("PUT failed: {}", key))?;
    Ok(true)
}

// ---- CatalogWriter ----

/// Manages per-key JSONL catalog files, creating directories on demand.
pub struct CatalogWriter {
    writers: HashMap<String, BufWriter<File>>,
    base_dir: PathBuf,
}

impl CatalogWriter {
    pub fn new(base_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("Failed to create catalog base dir {:?}", base_dir))?;
        Ok(Self { writers: HashMap::new(), base_dir })
    }

    pub fn write_entry(&mut self, catalog_key: &str, entry: &CatalogEntry) -> Result<()> {
        if !self.writers.contains_key(catalog_key) {
            let dir = self.base_dir.join(catalog_key);
            fs::create_dir_all(&dir)
                .with_context(|| format!("Failed to create catalog dir {:?}", dir))?;
            let path = dir.join("chunk.jsonl");
            let f = File::create(&path)
                .with_context(|| format!("Failed to create {:?}", path))?;
            self.writers.insert(
                catalog_key.to_string(),
                BufWriter::with_capacity(64 * 1024, f),
            );
        }
        let w = self.writers.get_mut(catalog_key).unwrap();
        let line = serde_json::to_string(entry)
            .with_context(|| "Failed to serialize catalog entry")?;
        writeln!(w, "{}", line)
            .with_context(|| "Failed to write catalog entry")?;
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        for (cc, w) in &mut self.writers {
            w.flush()
                .with_context(|| format!("Failed to flush catalog writer for {}", cc))?;
        }
        Ok(())
    }
}
