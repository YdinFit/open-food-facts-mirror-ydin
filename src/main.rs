use anyhow::{Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, StringRecord};
use flate2::read::GzDecoder;
use iso3166::{Country, LIST};
use open_food_facts_mirror::{
    setup_s3_client, upload_if_changed, AminoAcids, Breakdown, CatalogEntry, CatalogWriter,
    FatBreakdown, MacroNutrients, Minerals, OtherNutrients, Product, R2Config, Vitamins,
};
use rayon::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

// ---- CLI Args ----
#[derive(Parser, Debug)]
#[command(name = "process_chunk")]
struct Args {
    /// Path to products.csv.gz
    #[arg(long, default_value = "food_facts_raw_data/products.csv.gz")]
    input_file: PathBuf,

    /// Skip first N data rows (after header)
    #[arg(long, default_value_t = 0)]
    skip: usize,

    /// Process at most M rows (use 9999999 for "rest of file")
    #[arg(long, default_value_t = 9_999_999)]
    take: usize,

    /// Directory for partial catalogs + changed_codes.txt
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

const CSV_SEPARATOR: u8 = b'\t';
const BATCH_SIZE: usize = 10_000;

// ---- Column Index ----
struct ColumnIndex {
    product_name: Option<usize>,
    generic_name: Option<usize>,
    ingredients_text: Option<usize>,
    brands: Option<usize>,
    main_category: Option<usize>,
    countries: Option<usize>,
    serving_size: Option<usize>,
    serving_quantity: Option<usize>,
    energy_kcal_100g: Option<usize>,
    energy_kj_100g: Option<usize>,
    carbohydrates_100g: Option<usize>,
    fat_100g: Option<usize>,
    proteins_100g: Option<usize>,
    sugars_100g: Option<usize>,
    fiber_100g: Option<usize>,
    salt_100g: Option<usize>,
    added_sugars_100g: Option<usize>,
    sucrose_100g: Option<usize>,
    glucose_100g: Option<usize>,
    fructose_100g: Option<usize>,
    galactose_100g: Option<usize>,
    lactose_100g: Option<usize>,
    maltose_100g: Option<usize>,
    maltodextrins_100g: Option<usize>,
    psicose_100g: Option<usize>,
    starch_100g: Option<usize>,
    polyols_100g: Option<usize>,
    erythritol_100g: Option<usize>,
    isomalt_100g: Option<usize>,
    maltitol_100g: Option<usize>,
    sorbitol_100g: Option<usize>,
    soluble_fiber_100g: Option<usize>,
    insoluble_fiber_100g: Option<usize>,
    polydextrose_100g: Option<usize>,
    vitamin_a_100g: Option<usize>,
    beta_carotene_100g: Option<usize>,
    vitamin_d_100g: Option<usize>,
    vitamin_e_100g: Option<usize>,
    vitamin_k_100g: Option<usize>,
    vitamin_c_100g: Option<usize>,
    vitamin_b1_100g: Option<usize>,
    vitamin_b2_100g: Option<usize>,
    vitamin_pp_100g: Option<usize>,
    vitamin_b6_100g: Option<usize>,
    vitamin_b9_100g: Option<usize>,
    folates_100g: Option<usize>,
    vitamin_b12_100g: Option<usize>,
    biotin_100g: Option<usize>,
    pantothenic_acid_100g: Option<usize>,
    choline_100g: Option<usize>,
    phylloquinone_100g: Option<usize>,
    inositol_100g: Option<usize>,
    sodium_100g: Option<usize>,
    calcium_100g: Option<usize>,
    phosphorus_100g: Option<usize>,
    iron_100g: Option<usize>,
    magnesium_100g: Option<usize>,
    zinc_100g: Option<usize>,
    copper_100g: Option<usize>,
    manganese_100g: Option<usize>,
    fluoride_100g: Option<usize>,
    selenium_100g: Option<usize>,
    chromium_100g: Option<usize>,
    molybdenum_100g: Option<usize>,
    iodine_100g: Option<usize>,
    potassium_100g: Option<usize>,
    chloride_100g: Option<usize>,
    silica_100g: Option<usize>,
    bicarbonate_100g: Option<usize>,
    sulphate_100g: Option<usize>,
    nitrate_100g: Option<usize>,
    saturated_fat_100g: Option<usize>,
    unsaturated_fat_100g: Option<usize>,
    monounsaturated_fat_100g: Option<usize>,
    polyunsaturated_fat_100g: Option<usize>,
    trans_fat_100g: Option<usize>,
    cholesterol_100g: Option<usize>,
    omega_3_fat_100g: Option<usize>,
    omega_6_fat_100g: Option<usize>,
    omega_9_fat_100g: Option<usize>,
    alpha_linolenic_acid_100g: Option<usize>,
    eicosapentaenoic_acid_100g: Option<usize>,
    docosahexaenoic_acid_100g: Option<usize>,
    linoleic_acid_100g: Option<usize>,
    arachidonic_acid_100g: Option<usize>,
    gamma_linolenic_acid_100g: Option<usize>,
    dihomo_gamma_linolenic_acid_100g: Option<usize>,
    oleic_acid_100g: Option<usize>,
    elaidic_acid_100g: Option<usize>,
    gondoic_acid_100g: Option<usize>,
    mead_acid_100g: Option<usize>,
    erucic_acid_100g: Option<usize>,
    nervonic_acid_100g: Option<usize>,
    butyric_acid_100g: Option<usize>,
    caproic_acid_100g: Option<usize>,
    caprylic_acid_100g: Option<usize>,
    capric_acid_100g: Option<usize>,
    lauric_acid_100g: Option<usize>,
    myristic_acid_100g: Option<usize>,
    palmitic_acid_100g: Option<usize>,
    stearic_acid_100g: Option<usize>,
    arachidic_acid_100g: Option<usize>,
    behenic_acid_100g: Option<usize>,
    lignoceric_acid_100g: Option<usize>,
    cerotic_acid_100g: Option<usize>,
    montanic_acid_100g: Option<usize>,
    melissic_acid_100g: Option<usize>,
    caffeine_100g: Option<usize>,
    taurine_100g: Option<usize>,
    carnitine_100g: Option<usize>,
    beta_glucan_100g: Option<usize>,
    alcohol_100g: Option<usize>,
    nucleotides_100g: Option<usize>,
    casein_100g: Option<usize>,
    serum_proteins_100g: Option<usize>,
    methylsulfonylmethane_100g: Option<usize>,
    energy_from_fat_100g: Option<usize>,
    added_salt_100g: Option<usize>,
    histidine_100g: Option<usize>,
    isoleucine_100g: Option<usize>,
    leucine_100g: Option<usize>,
    lysine_100g: Option<usize>,
    methionine_100g: Option<usize>,
    phenylalanine_100g: Option<usize>,
    threonine_100g: Option<usize>,
    tryptophan_100g: Option<usize>,
    valine_100g: Option<usize>,
    alanine_100g: Option<usize>,
    arginine_100g: Option<usize>,
    aspartic_acid_100g: Option<usize>,
    cysteine_100g: Option<usize>,
    glutamic_acid_100g: Option<usize>,
    glycine_100g: Option<usize>,
    proline_100g: Option<usize>,
    serine_100g: Option<usize>,
    tyrosine_100g: Option<usize>,
    hydroxyproline_100g: Option<usize>,
    cystine_100g: Option<usize>,
}

impl ColumnIndex {
    fn from_headers(headers: &StringRecord) -> Self {
        let mut idx = ColumnIndex {
            product_name: None, generic_name: None, ingredients_text: None, brands: None,
            main_category: None, countries: None, serving_size: None, serving_quantity: None,
            energy_kcal_100g: None, energy_kj_100g: None, carbohydrates_100g: None,
            fat_100g: None, proteins_100g: None, sugars_100g: None, fiber_100g: None,
            salt_100g: None, added_sugars_100g: None, sucrose_100g: None, glucose_100g: None,
            fructose_100g: None, galactose_100g: None, lactose_100g: None, maltose_100g: None,
            maltodextrins_100g: None, psicose_100g: None, starch_100g: None, polyols_100g: None,
            erythritol_100g: None, isomalt_100g: None, maltitol_100g: None, sorbitol_100g: None,
            soluble_fiber_100g: None, insoluble_fiber_100g: None, polydextrose_100g: None,
            vitamin_a_100g: None, beta_carotene_100g: None, vitamin_d_100g: None,
            vitamin_e_100g: None, vitamin_k_100g: None, vitamin_c_100g: None,
            vitamin_b1_100g: None, vitamin_b2_100g: None, vitamin_pp_100g: None,
            vitamin_b6_100g: None, vitamin_b9_100g: None, folates_100g: None,
            vitamin_b12_100g: None, biotin_100g: None, pantothenic_acid_100g: None,
            choline_100g: None, phylloquinone_100g: None, inositol_100g: None,
            sodium_100g: None, calcium_100g: None, phosphorus_100g: None, iron_100g: None,
            magnesium_100g: None, zinc_100g: None, copper_100g: None, manganese_100g: None,
            fluoride_100g: None, selenium_100g: None, chromium_100g: None, molybdenum_100g: None,
            iodine_100g: None, potassium_100g: None, chloride_100g: None, silica_100g: None,
            bicarbonate_100g: None, sulphate_100g: None, nitrate_100g: None,
            saturated_fat_100g: None, unsaturated_fat_100g: None, monounsaturated_fat_100g: None,
            polyunsaturated_fat_100g: None, trans_fat_100g: None, cholesterol_100g: None,
            omega_3_fat_100g: None, omega_6_fat_100g: None, omega_9_fat_100g: None,
            alpha_linolenic_acid_100g: None, eicosapentaenoic_acid_100g: None,
            docosahexaenoic_acid_100g: None, linoleic_acid_100g: None,
            arachidonic_acid_100g: None, gamma_linolenic_acid_100g: None,
            dihomo_gamma_linolenic_acid_100g: None, oleic_acid_100g: None,
            elaidic_acid_100g: None, gondoic_acid_100g: None, mead_acid_100g: None,
            erucic_acid_100g: None, nervonic_acid_100g: None, butyric_acid_100g: None,
            caproic_acid_100g: None, caprylic_acid_100g: None, capric_acid_100g: None,
            lauric_acid_100g: None, myristic_acid_100g: None, palmitic_acid_100g: None,
            stearic_acid_100g: None, arachidic_acid_100g: None, behenic_acid_100g: None,
            lignoceric_acid_100g: None, cerotic_acid_100g: None, montanic_acid_100g: None,
            melissic_acid_100g: None, caffeine_100g: None, taurine_100g: None,
            carnitine_100g: None, beta_glucan_100g: None, alcohol_100g: None,
            nucleotides_100g: None, casein_100g: None, serum_proteins_100g: None,
            methylsulfonylmethane_100g: None, energy_from_fat_100g: None, added_salt_100g: None,
            histidine_100g: None, isoleucine_100g: None, leucine_100g: None, lysine_100g: None,
            methionine_100g: None, phenylalanine_100g: None, threonine_100g: None,
            tryptophan_100g: None, valine_100g: None, alanine_100g: None, arginine_100g: None,
            aspartic_acid_100g: None, cysteine_100g: None, glutamic_acid_100g: None,
            glycine_100g: None, proline_100g: None, serine_100g: None, tyrosine_100g: None,
            hydroxyproline_100g: None, cystine_100g: None,
        };
        for (i, header) in headers.iter().enumerate() {
            match header {
                "product_name" => idx.product_name = Some(i),
                "generic_name" => idx.generic_name = Some(i),
                "ingredients_text" => idx.ingredients_text = Some(i),
                "brands" => idx.brands = Some(i),
                "main_category" => idx.main_category = Some(i),
                "countries" => idx.countries = Some(i),
                "serving_size" => idx.serving_size = Some(i),
                "serving_quantity" => idx.serving_quantity = Some(i),
                "energy-kcal_100g" => idx.energy_kcal_100g = Some(i),
                "energy-kj_100g" => idx.energy_kj_100g = Some(i),
                "carbohydrates_100g" => idx.carbohydrates_100g = Some(i),
                "fat_100g" => idx.fat_100g = Some(i),
                "proteins_100g" => idx.proteins_100g = Some(i),
                "sugars_100g" => idx.sugars_100g = Some(i),
                "fiber_100g" => idx.fiber_100g = Some(i),
                "salt_100g" => idx.salt_100g = Some(i),
                "added-sugars_100g" => idx.added_sugars_100g = Some(i),
                "sucrose_100g" => idx.sucrose_100g = Some(i),
                "glucose_100g" => idx.glucose_100g = Some(i),
                "fructose_100g" => idx.fructose_100g = Some(i),
                "galactose_100g" => idx.galactose_100g = Some(i),
                "lactose_100g" => idx.lactose_100g = Some(i),
                "maltose_100g" => idx.maltose_100g = Some(i),
                "maltodextrins_100g" => idx.maltodextrins_100g = Some(i),
                "psicose_100g" => idx.psicose_100g = Some(i),
                "starch_100g" => idx.starch_100g = Some(i),
                "polyols_100g" => idx.polyols_100g = Some(i),
                "erythritol_100g" => idx.erythritol_100g = Some(i),
                "isomalt_100g" => idx.isomalt_100g = Some(i),
                "maltitol_100g" => idx.maltitol_100g = Some(i),
                "sorbitol_100g" => idx.sorbitol_100g = Some(i),
                "soluble-fiber_100g" => idx.soluble_fiber_100g = Some(i),
                "insoluble-fiber_100g" => idx.insoluble_fiber_100g = Some(i),
                "polydextrose_100g" => idx.polydextrose_100g = Some(i),
                "vitamin-a_100g" => idx.vitamin_a_100g = Some(i),
                "beta-carotene_100g" => idx.beta_carotene_100g = Some(i),
                "vitamin-d_100g" => idx.vitamin_d_100g = Some(i),
                "vitamin-e_100g" => idx.vitamin_e_100g = Some(i),
                "vitamin-k_100g" => idx.vitamin_k_100g = Some(i),
                "vitamin-c_100g" => idx.vitamin_c_100g = Some(i),
                "vitamin-b1_100g" => idx.vitamin_b1_100g = Some(i),
                "vitamin-b2_100g" => idx.vitamin_b2_100g = Some(i),
                "vitamin-pp_100g" => idx.vitamin_pp_100g = Some(i),
                "vitamin-b6_100g" => idx.vitamin_b6_100g = Some(i),
                "vitamin-b9_100g" => idx.vitamin_b9_100g = Some(i),
                "folates_100g" => idx.folates_100g = Some(i),
                "vitamin-b12_100g" => idx.vitamin_b12_100g = Some(i),
                "biotin_100g" => idx.biotin_100g = Some(i),
                "pantothenic-acid_100g" => idx.pantothenic_acid_100g = Some(i),
                "choline_100g" => idx.choline_100g = Some(i),
                "phylloquinone_100g" => idx.phylloquinone_100g = Some(i),
                "inositol_100g" => idx.inositol_100g = Some(i),
                "sodium_100g" => idx.sodium_100g = Some(i),
                "calcium_100g" => idx.calcium_100g = Some(i),
                "phosphorus_100g" => idx.phosphorus_100g = Some(i),
                "iron_100g" => idx.iron_100g = Some(i),
                "magnesium_100g" => idx.magnesium_100g = Some(i),
                "zinc_100g" => idx.zinc_100g = Some(i),
                "copper_100g" => idx.copper_100g = Some(i),
                "manganese_100g" => idx.manganese_100g = Some(i),
                "fluoride_100g" => idx.fluoride_100g = Some(i),
                "selenium_100g" => idx.selenium_100g = Some(i),
                "chromium_100g" => idx.chromium_100g = Some(i),
                "molybdenum_100g" => idx.molybdenum_100g = Some(i),
                "iodine_100g" => idx.iodine_100g = Some(i),
                "potassium_100g" => idx.potassium_100g = Some(i),
                "chloride_100g" => idx.chloride_100g = Some(i),
                "silica_100g" => idx.silica_100g = Some(i),
                "bicarbonate_100g" => idx.bicarbonate_100g = Some(i),
                "sulphate_100g" => idx.sulphate_100g = Some(i),
                "nitrate_100g" => idx.nitrate_100g = Some(i),
                "saturated-fat_100g" => idx.saturated_fat_100g = Some(i),
                "unsaturated-fat_100g" => idx.unsaturated_fat_100g = Some(i),
                "monounsaturated-fat_100g" => idx.monounsaturated_fat_100g = Some(i),
                "polyunsaturated-fat_100g" => idx.polyunsaturated_fat_100g = Some(i),
                "trans-fat_100g" => idx.trans_fat_100g = Some(i),
                "cholesterol_100g" => idx.cholesterol_100g = Some(i),
                "omega-3-fat_100g" => idx.omega_3_fat_100g = Some(i),
                "omega-6-fat_100g" => idx.omega_6_fat_100g = Some(i),
                "omega-9-fat_100g" => idx.omega_9_fat_100g = Some(i),
                "alpha-linolenic-acid_100g" => idx.alpha_linolenic_acid_100g = Some(i),
                "eicosapentaenoic-acid_100g" => idx.eicosapentaenoic_acid_100g = Some(i),
                "docosahexaenoic-acid_100g" => idx.docosahexaenoic_acid_100g = Some(i),
                "linoleic-acid_100g" => idx.linoleic_acid_100g = Some(i),
                "arachidonic-acid_100g" => idx.arachidonic_acid_100g = Some(i),
                "gamma-linolenic-acid_100g" => idx.gamma_linolenic_acid_100g = Some(i),
                "dihomo-gamma-linolenic-acid_100g" => idx.dihomo_gamma_linolenic_acid_100g = Some(i),
                "oleic-acid_100g" => idx.oleic_acid_100g = Some(i),
                "elaidic-acid_100g" => idx.elaidic_acid_100g = Some(i),
                "gondoic-acid_100g" => idx.gondoic_acid_100g = Some(i),
                "mead-acid_100g" => idx.mead_acid_100g = Some(i),
                "erucic-acid_100g" => idx.erucic_acid_100g = Some(i),
                "nervonic-acid_100g" => idx.nervonic_acid_100g = Some(i),
                "butyric-acid_100g" => idx.butyric_acid_100g = Some(i),
                "caproic-acid_100g" => idx.caproic_acid_100g = Some(i),
                "caprylic-acid_100g" => idx.caprylic_acid_100g = Some(i),
                "capric-acid_100g" => idx.capric_acid_100g = Some(i),
                "lauric-acid_100g" => idx.lauric_acid_100g = Some(i),
                "myristic-acid_100g" => idx.myristic_acid_100g = Some(i),
                "palmitic-acid_100g" => idx.palmitic_acid_100g = Some(i),
                "stearic-acid_100g" => idx.stearic_acid_100g = Some(i),
                "arachidic-acid_100g" => idx.arachidic_acid_100g = Some(i),
                "behenic-acid_100g" => idx.behenic_acid_100g = Some(i),
                "lignoceric-acid_100g" => idx.lignoceric_acid_100g = Some(i),
                "cerotic-acid_100g" => idx.cerotic_acid_100g = Some(i),
                "montanic-acid_100g" => idx.montanic_acid_100g = Some(i),
                "melissic-acid_100g" => idx.melissic_acid_100g = Some(i),
                "caffeine_100g" => idx.caffeine_100g = Some(i),
                "taurine_100g" => idx.taurine_100g = Some(i),
                "carnitine_100g" => idx.carnitine_100g = Some(i),
                "beta-glucan_100g" => idx.beta_glucan_100g = Some(i),
                "alcohol_100g" => idx.alcohol_100g = Some(i),
                "nucleotides_100g" => idx.nucleotides_100g = Some(i),
                "casein_100g" => idx.casein_100g = Some(i),
                "serum-proteins_100g" => idx.serum_proteins_100g = Some(i),
                "methylsulfonylmethane_100g" => idx.methylsulfonylmethane_100g = Some(i),
                "energy-from-fat_100g" => idx.energy_from_fat_100g = Some(i),
                "added-salt_100g" => idx.added_salt_100g = Some(i),
                "histidine_100g" => idx.histidine_100g = Some(i),
                "isoleucine_100g" => idx.isoleucine_100g = Some(i),
                "leucine_100g" => idx.leucine_100g = Some(i),
                "lysine_100g" => idx.lysine_100g = Some(i),
                "methionine_100g" => idx.methionine_100g = Some(i),
                "phenylalanine_100g" => idx.phenylalanine_100g = Some(i),
                "threonine_100g" => idx.threonine_100g = Some(i),
                "tryptophan_100g" => idx.tryptophan_100g = Some(i),
                "valine_100g" => idx.valine_100g = Some(i),
                "alanine_100g" => idx.alanine_100g = Some(i),
                "arginine_100g" => idx.arginine_100g = Some(i),
                "aspartic-acid_100g" => idx.aspartic_acid_100g = Some(i),
                "cysteine_100g" => idx.cysteine_100g = Some(i),
                "glutamic-acid_100g" => idx.glutamic_acid_100g = Some(i),
                "glycine_100g" => idx.glycine_100g = Some(i),
                "proline_100g" => idx.proline_100g = Some(i),
                "serine_100g" => idx.serine_100g = Some(i),
                "tyrosine_100g" => idx.tyrosine_100g = Some(i),
                "hydroxyproline_100g" => idx.hydroxyproline_100g = Some(i),
                "cystine_100g" => idx.cystine_100g = Some(i),
                _ => {}
            }
        }
        idx
    }
}

// ---- Helpers ----
fn get_field<'a>(record: &'a StringRecord, idx: Option<usize>) -> Option<&'a str> {
    idx.and_then(|i| record.get(i)).filter(|s| !s.is_empty())
}

fn to_num(v: Option<&str>) -> Option<f64> {
    let v = v?;
    let cleaned = v.replace(' ', "").replace(',', ".");
    cleaned.parse().ok().filter(|n: &f64| n.is_finite())
}

fn serving_qty_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"([\d.,]+)\s*(g|gram|grams|ml|milliliter|milliliters)?").unwrap())
}

fn serving_unit_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b(ml|milliliters?|g|grams?)\b").unwrap())
}

fn parse_serving(record: &StringRecord, col_index: &ColumnIndex) -> (Option<f64>, Option<String>) {
    let raw_size_str = get_field(record, col_index.serving_size);
    let qty = to_num(get_field(record, col_index.serving_quantity)).or_else(|| {
        raw_size_str.and_then(|size_str| {
            serving_qty_regex()
                .captures(size_str)
                .and_then(|c| c.get(1))
                .and_then(|m| {
                    let cleaned = m.as_str().replace(',', ".");
                    cleaned.parse::<f64>().ok()
                })
        })
    });
    let unit = raw_size_str.and_then(|size_str| {
        serving_unit_regex()
            .captures(size_str)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_lowercase())
    });
    (qty, unit)
}

// ---- Country Cache ----
fn build_country_cache() -> HashMap<String, String> {
    let mut cache = HashMap::new();
    for country in LIST {
        cache.insert(country.name.to_lowercase(), country.alpha2.to_lowercase());
        cache.insert(country.alpha2.to_lowercase(), country.alpha2.to_lowercase());
    }
    let aliases: &[(&str, &str)] = &[
        ("united states", "us"), ("usa", "us"), ("united states of america", "us"),
        ("united kingdom", "gb"), ("uk", "gb"), ("great britain", "gb"), ("britain", "gb"),
        ("germany", "de"), ("deutschland", "de"),
        ("netherlands", "nl"), ("holland", "nl"),
        ("switzerland", "ch"), ("schweiz", "ch"),
        ("brazil", "br"), ("brasil", "br"),
        ("south korea", "kr"), ("korea", "kr"),
        ("czech republic", "cz"), ("czechia", "cz"),
        ("congo", "cg"), ("democratic republic of the congo", "cd"), ("drc", "cd"),
        ("cape verde", "cv"), ("cabo verde", "cv"),
        ("ivory coast", "ci"), ("cote d'ivoire", "ci"),
        ("myanmar", "mm"), ("burma", "mm"),
        ("united arab emirates", "ae"), ("uae", "ae"),
        ("french guiana", "gf"), ("north macedonia", "mk"), ("kosovo", "xk"),
        ("world", "global"),
    ];
    for &(alias, code) in aliases {
        cache.insert(alias.to_string(), code.to_string());
    }
    cache
}

fn map_country_to_iso_code(name: &str, cache: &HashMap<String, String>) -> String {
    let country_name = if let Some(colon_pos) = name.find(':') {
        name[colon_pos + 1..].replace('-', " ")
    } else {
        name.to_string()
    };
    let search_name = country_name.to_lowercase();
    if let Some(code) = cache.get(&search_name) {
        return code.clone();
    }
    if search_name.len() == 2 && search_name.chars().all(|c| c.is_ascii_alphabetic()) {
        if Country::from_alpha2_ignore_case(&search_name).is_some() {
            return search_name;
        }
    }
    for country in LIST {
        let cn = country.name.to_lowercase();
        if cn.contains(&search_name) || search_name.contains(&cn) {
            return country.alpha2.to_lowercase();
        }
    }
    if search_name == "world" {
        return "global".to_string();
    }
    "unknown".to_string()
}

fn normalize_country_codes(countries_str: &str, cache: &HashMap<String, String>) -> Vec<String> {
    let mut codes = Vec::new();
    let countries: Vec<&str> = countries_str
        .split(|c| c == ',' || c == ';' || c == '|')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    for country in countries {
        let normalized = country.to_lowercase();
        if normalized.contains("world") {
            if !codes.contains(&"global".to_string()) {
                codes.push("global".to_string());
            }
            continue;
        }
        let iso_code = map_country_to_iso_code(&normalized, cache);
        if !codes.contains(&iso_code) {
            codes.push(iso_code);
        }
    }
    if codes.is_empty() {
        codes.push("unknown".to_string());
    }
    codes
}

// ---- Record Parsing ----
struct ParsedRecord {
    code: String,
    json_bytes: Vec<u8>,
    catalog_entries: Vec<(CatalogEntry, String)>,
}

fn parse_single_record(
    record: &StringRecord,
    col_index: &ColumnIndex,
    country_cache: &HashMap<String, String>,
) -> Option<ParsedRecord> {
    let code = record.get(0).unwrap_or("").replace(|c: char| !c.is_ascii_digit(), "");
    if code.is_empty() {
        return None;
    }

    let name = get_field(record, col_index.product_name)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let generic_name = get_field(record, col_index.generic_name)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let ingredients_text = get_field(record, col_index.ingredients_text)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let brand = get_field(record, col_index.brands)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let main_category = get_field(record, col_index.main_category)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let countries_str = get_field(record, col_index.countries).unwrap_or("");
    let country_codes = normalize_country_codes(countries_str, country_cache);

    let (serving_size, serving_unit) = parse_serving(record, col_index);

    let energy_kcal = to_num(get_field(record, col_index.energy_kcal_100g));
    let carbohydrates = to_num(get_field(record, col_index.carbohydrates_100g));
    let fat = to_num(get_field(record, col_index.fat_100g));
    let proteins = to_num(get_field(record, col_index.proteins_100g));

    // Validation: must have energy_kcal AND at least one macro
    if energy_kcal.is_none() || (carbohydrates.is_none() && fat.is_none() && proteins.is_none()) {
        return None;
    }

    let macros = MacroNutrients {
        energy_kcal,
        energy_kj: to_num(get_field(record, col_index.energy_kj_100g)),
        carbohydrates,
        fat,
        proteins,
        sugars: to_num(get_field(record, col_index.sugars_100g)),
        fiber: to_num(get_field(record, col_index.fiber_100g)),
        salt: to_num(get_field(record, col_index.salt_100g)),
        added_sugars: to_num(get_field(record, col_index.added_sugars_100g)),
        sucrose: to_num(get_field(record, col_index.sucrose_100g)),
        glucose: to_num(get_field(record, col_index.glucose_100g)),
        fructose: to_num(get_field(record, col_index.fructose_100g)),
        galactose: to_num(get_field(record, col_index.galactose_100g)),
        lactose: to_num(get_field(record, col_index.lactose_100g)),
        maltose: to_num(get_field(record, col_index.maltose_100g)),
        maltodextrins: to_num(get_field(record, col_index.maltodextrins_100g)),
        psicose: to_num(get_field(record, col_index.psicose_100g)),
        starch: to_num(get_field(record, col_index.starch_100g)),
        polyols: to_num(get_field(record, col_index.polyols_100g)),
        erythritol: to_num(get_field(record, col_index.erythritol_100g)),
        isomalt: to_num(get_field(record, col_index.isomalt_100g)),
        maltitol: to_num(get_field(record, col_index.maltitol_100g)),
        sorbitol: to_num(get_field(record, col_index.sorbitol_100g)),
        soluble_fiber: to_num(get_field(record, col_index.soluble_fiber_100g)),
        insoluble_fiber: to_num(get_field(record, col_index.insoluble_fiber_100g)),
        polydextrose: to_num(get_field(record, col_index.polydextrose_100g)),
    };

    let vitamins = Vitamins {
        vitamin_a: to_num(get_field(record, col_index.vitamin_a_100g)),
        beta_carotene: to_num(get_field(record, col_index.beta_carotene_100g)),
        vitamin_d: to_num(get_field(record, col_index.vitamin_d_100g)),
        vitamin_e: to_num(get_field(record, col_index.vitamin_e_100g)),
        vitamin_k: to_num(get_field(record, col_index.vitamin_k_100g)),
        vitamin_c: to_num(get_field(record, col_index.vitamin_c_100g)),
        vitamin_b1: to_num(get_field(record, col_index.vitamin_b1_100g)),
        vitamin_b2: to_num(get_field(record, col_index.vitamin_b2_100g)),
        vitamin_pp: to_num(get_field(record, col_index.vitamin_pp_100g)),
        vitamin_b6: to_num(get_field(record, col_index.vitamin_b6_100g)),
        vitamin_b9: to_num(get_field(record, col_index.vitamin_b9_100g)),
        folates: to_num(get_field(record, col_index.folates_100g)),
        vitamin_b12: to_num(get_field(record, col_index.vitamin_b12_100g)),
        biotin: to_num(get_field(record, col_index.biotin_100g)),
        pantothenic_acid: to_num(get_field(record, col_index.pantothenic_acid_100g)),
        choline: to_num(get_field(record, col_index.choline_100g)),
        phylloquinone: to_num(get_field(record, col_index.phylloquinone_100g)),
        inositol: to_num(get_field(record, col_index.inositol_100g)),
    };

    let minerals = Minerals {
        sodium: to_num(get_field(record, col_index.sodium_100g)),
        calcium: to_num(get_field(record, col_index.calcium_100g)),
        phosphorus: to_num(get_field(record, col_index.phosphorus_100g)),
        iron: to_num(get_field(record, col_index.iron_100g)),
        magnesium: to_num(get_field(record, col_index.magnesium_100g)),
        zinc: to_num(get_field(record, col_index.zinc_100g)),
        copper: to_num(get_field(record, col_index.copper_100g)),
        manganese: to_num(get_field(record, col_index.manganese_100g)),
        fluoride: to_num(get_field(record, col_index.fluoride_100g)),
        selenium: to_num(get_field(record, col_index.selenium_100g)),
        chromium: to_num(get_field(record, col_index.chromium_100g)),
        molybdenum: to_num(get_field(record, col_index.molybdenum_100g)),
        iodine: to_num(get_field(record, col_index.iodine_100g)),
        potassium: to_num(get_field(record, col_index.potassium_100g)),
        chloride: to_num(get_field(record, col_index.chloride_100g)),
        silica: to_num(get_field(record, col_index.silica_100g)),
        bicarbonate: to_num(get_field(record, col_index.bicarbonate_100g)),
        sulphate: to_num(get_field(record, col_index.sulphate_100g)),
        nitrate: to_num(get_field(record, col_index.nitrate_100g)),
    };

    let fats = FatBreakdown {
        saturated: to_num(get_field(record, col_index.saturated_fat_100g)),
        unsaturated: to_num(get_field(record, col_index.unsaturated_fat_100g)),
        monounsaturated: to_num(get_field(record, col_index.monounsaturated_fat_100g)),
        polyunsaturated: to_num(get_field(record, col_index.polyunsaturated_fat_100g)),
        trans: to_num(get_field(record, col_index.trans_fat_100g)),
        cholesterol: to_num(get_field(record, col_index.cholesterol_100g)),
        omega_3: to_num(get_field(record, col_index.omega_3_fat_100g)),
        omega_6: to_num(get_field(record, col_index.omega_6_fat_100g)),
        omega_9: to_num(get_field(record, col_index.omega_9_fat_100g)),
        alpha_linolenic_acid: to_num(get_field(record, col_index.alpha_linolenic_acid_100g)),
        eicosapentaenoic_acid: to_num(get_field(record, col_index.eicosapentaenoic_acid_100g)),
        docosahexaenoic_acid: to_num(get_field(record, col_index.docosahexaenoic_acid_100g)),
        linoleic_acid: to_num(get_field(record, col_index.linoleic_acid_100g)),
        arachidonic_acid: to_num(get_field(record, col_index.arachidonic_acid_100g)),
        gamma_linolenic_acid: to_num(get_field(record, col_index.gamma_linolenic_acid_100g)),
        dihomo_gamma_linolenic_acid: to_num(get_field(record, col_index.dihomo_gamma_linolenic_acid_100g)),
        oleic_acid: to_num(get_field(record, col_index.oleic_acid_100g)),
        elaidic_acid: to_num(get_field(record, col_index.elaidic_acid_100g)),
        gondoic_acid: to_num(get_field(record, col_index.gondoic_acid_100g)),
        mead_acid: to_num(get_field(record, col_index.mead_acid_100g)),
        erucic_acid: to_num(get_field(record, col_index.erucic_acid_100g)),
        nervonic_acid: to_num(get_field(record, col_index.nervonic_acid_100g)),
        butyric_acid: to_num(get_field(record, col_index.butyric_acid_100g)),
        caproic_acid: to_num(get_field(record, col_index.caproic_acid_100g)),
        caprylic_acid: to_num(get_field(record, col_index.caprylic_acid_100g)),
        capric_acid: to_num(get_field(record, col_index.capric_acid_100g)),
        lauric_acid: to_num(get_field(record, col_index.lauric_acid_100g)),
        myristic_acid: to_num(get_field(record, col_index.myristic_acid_100g)),
        palmitic_acid: to_num(get_field(record, col_index.palmitic_acid_100g)),
        stearic_acid: to_num(get_field(record, col_index.stearic_acid_100g)),
        arachidic_acid: to_num(get_field(record, col_index.arachidic_acid_100g)),
        behenic_acid: to_num(get_field(record, col_index.behenic_acid_100g)),
        lignoceric_acid: to_num(get_field(record, col_index.lignoceric_acid_100g)),
        cerotic_acid: to_num(get_field(record, col_index.cerotic_acid_100g)),
        montanic_acid: to_num(get_field(record, col_index.montanic_acid_100g)),
        melissic_acid: to_num(get_field(record, col_index.melissic_acid_100g)),
    };

    let other = OtherNutrients {
        caffeine: to_num(get_field(record, col_index.caffeine_100g)),
        taurine: to_num(get_field(record, col_index.taurine_100g)),
        carnitine: to_num(get_field(record, col_index.carnitine_100g)),
        beta_glucan: to_num(get_field(record, col_index.beta_glucan_100g)),
        alcohol: to_num(get_field(record, col_index.alcohol_100g)),
        nucleotides: to_num(get_field(record, col_index.nucleotides_100g)),
        casein: to_num(get_field(record, col_index.casein_100g)),
        serum_proteins: to_num(get_field(record, col_index.serum_proteins_100g)),
        methylsulfonylmethane: to_num(get_field(record, col_index.methylsulfonylmethane_100g)),
        energy_from_fat: to_num(get_field(record, col_index.energy_from_fat_100g)),
        added_salt: to_num(get_field(record, col_index.added_salt_100g)),
    };

    let amino_acids = AminoAcids {
        histidine: to_num(get_field(record, col_index.histidine_100g)),
        isoleucine: to_num(get_field(record, col_index.isoleucine_100g)),
        leucine: to_num(get_field(record, col_index.leucine_100g)),
        lysine: to_num(get_field(record, col_index.lysine_100g)),
        methionine: to_num(get_field(record, col_index.methionine_100g)),
        phenylalanine: to_num(get_field(record, col_index.phenylalanine_100g)),
        threonine: to_num(get_field(record, col_index.threonine_100g)),
        tryptophan: to_num(get_field(record, col_index.tryptophan_100g)),
        valine: to_num(get_field(record, col_index.valine_100g)),
        alanine: to_num(get_field(record, col_index.alanine_100g)),
        arginine: to_num(get_field(record, col_index.arginine_100g)),
        aspartic_acid: to_num(get_field(record, col_index.aspartic_acid_100g)),
        cysteine: to_num(get_field(record, col_index.cysteine_100g)),
        glutamic_acid: to_num(get_field(record, col_index.glutamic_acid_100g)),
        glycine: to_num(get_field(record, col_index.glycine_100g)),
        proline: to_num(get_field(record, col_index.proline_100g)),
        serine: to_num(get_field(record, col_index.serine_100g)),
        tyrosine: to_num(get_field(record, col_index.tyrosine_100g)),
        hydroxyproline: to_num(get_field(record, col_index.hydroxyproline_100g)),
        cystine: to_num(get_field(record, col_index.cystine_100g)),
    };

    // Extract catalog fields before moving macros into Breakdown
    let catalog_fiber = macros.fiber;
    let catalog_carbs = macros.carbohydrates;
    let catalog_fat = macros.fat;
    let catalog_protein = macros.proteins;

    let product = Product {
        code: code.clone(),
        source: "open-food-facts".to_string(),
        product_name: name.clone(),
        generic_name,
        ingredients_text,
        brands: brand.clone(),
        main_category,
        serving_size,
        serving_unit: serving_unit.clone(),
        breakdown: Breakdown { macros, vitamins, minerals, fats, other, amino_acids },
    };

    let json_bytes = serde_json::to_vec(&product).ok()?;

    let catalog_serving_size = serving_size.or(Some(100.0));
    let catalog_serving_unit = serving_unit.or_else(|| Some("g".to_string()));

    let catalog_entries = country_codes
        .into_iter()
        .map(|country_code| {
            let entry = CatalogEntry {
                code: code.clone(),
                name: name.clone(),
                brand: brand.clone(),
                country: Some(country_code.clone()),
                serving_size: catalog_serving_size,
                serving_unit: catalog_serving_unit.clone(),
                fiber: catalog_fiber,
                carbs: catalog_carbs,
                fat: catalog_fat,
                protein: catalog_protein,
            };
            (entry, country_code)
        })
        .collect();

    Some(ParsedRecord { code, json_bytes, catalog_entries })
}

// ---- Main ----
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("process_chunk starting");
    println!("  input:      {:?}", args.input_file);
    println!("  skip:       {}", args.skip);
    println!("  take:       {}", args.take);
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

    let catalog_base = args.output_dir.join("catalogs");
    let mut catalog_writer = CatalogWriter::new(catalog_base)?;

    let changed_path = args.output_dir.join("changed_codes.txt");
    let changed_file = File::create(&changed_path)
        .with_context(|| format!("Failed to create {:?}", changed_path))?;
    let mut changed_writer = BufWriter::new(changed_file);

    let country_cache = Arc::new(build_country_cache());

    let input_file = File::open(&args.input_file)
        .with_context(|| format!("Failed to open {:?}", args.input_file))?;
    let decoder = GzDecoder::new(input_file);
    let mut reader = ReaderBuilder::new()
        .delimiter(CSV_SEPARATOR)
        .flexible(true)
        .from_reader(decoder);

    let headers = reader.headers()?.clone();
    let col_index = Arc::new(ColumnIndex::from_headers(&headers));

    let mut records_iter = reader.records();

    // Skip first N rows
    for i in 0..args.skip {
        match records_iter.next() {
            Some(Ok(_)) => {}
            Some(Err(e)) => eprintln!("Warning skipping row {}: {}", i, e),
            None => {
                println!("EOF reached during skip at row {}", i);
                return Ok(());
            }
        }
    }

    let mut upload_tasks: JoinSet<Option<String>> = JoinSet::new();
    let mut batch: Vec<StringRecord> = Vec::with_capacity(BATCH_SIZE);
    let mut taken = 0usize;
    let mut total_processed = 0usize;
    let mut total_skipped = 0usize;
    let mut batch_num = 0usize;
    let start = Instant::now();

    loop {
        let remaining = args.take - taken;
        if remaining == 0 {
            break;
        }

        match records_iter.next() {
            Some(Ok(record)) => {
                batch.push(record);
                taken += 1;
            }
            Some(Err(e)) => {
                eprintln!("Warning: malformed record: {}", e);
                total_skipped += 1;
            }
            None => break,
        }

        let batch_full = batch.len() >= BATCH_SIZE;
        let last_batch = taken >= args.take;

        if (batch_full || last_batch) && !batch.is_empty() {
            batch_num += 1;
            let batch_to_process = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
            let col = col_index.clone();
            let cache = country_cache.clone();

            // Parse with Rayon in blocking context
            let parsed: Vec<ParsedRecord> = tokio::task::block_in_place(|| {
                batch_to_process
                    .par_iter()
                    .filter_map(|record| parse_single_record(record, &col, &cache))
                    .collect()
            });

            let batch_processed = parsed.len();
            let batch_skipped = batch_to_process.len() - batch_processed;
            total_processed += batch_processed;
            total_skipped += batch_skipped;

            for pr in parsed {
                // Write catalog entries
                for (entry, country_code) in &pr.catalog_entries {
                    catalog_writer.write_entry(country_code, entry)?;
                }

                // Spawn R2 upload — returns Some(code) if uploaded, None if unchanged
                let sem = semaphore.clone();
                let client = s3_client.clone();
                let bkt = bucket.clone();
                let code = pr.code.clone();
                let key = format!("products/{}.json", pr.code);
                let body = pr.json_bytes;
                upload_tasks.spawn(async move {
                    let _permit = sem.acquire().await.unwrap();
                    match upload_if_changed(&client, &bkt, &key, body).await {
                        Ok(true) => Some(code),
                        Ok(false) => None,
                        Err(e) => {
                            eprintln!("Upload error {}: {}", key, e);
                            None
                        }
                    }
                });
            }

            let elapsed = start.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 { total_processed as f64 / elapsed } else { 0.0 };
            println!(
                "Batch {}: processed={} skipped={} total={} rate={:.0}/s uploads_pending={}",
                batch_num,
                batch_processed,
                batch_skipped,
                total_processed,
                rate,
                upload_tasks.len()
            );
        }
    }

    // Flush catalog writers
    catalog_writer.flush_all()?;

    // Wait for all uploads to complete, collect changed codes
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

    let elapsed = start.elapsed().as_secs_f64();
    println!("\nDone:");
    println!("  Processed:  {}", total_processed);
    println!("  Skipped:    {}", total_skipped);
    println!("  Uploaded:   {}", total_uploaded);
    println!("  Unchanged:  {}", total_unchanged);
    println!("  Time:       {:.1}s", elapsed);
    println!("  Rate:       {:.0} products/s", total_processed as f64 / elapsed);

    Ok(())
}
