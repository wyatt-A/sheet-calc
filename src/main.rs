use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
use std::{fs::File, io::Read};
use clap::Parser;
use ndarray::{Array2, Axis};
use ndarray::s;
use regex::Regex;
use rayon::prelude::*;
use serde::{Serialize,Deserialize};
use sheet_calc::SpreadSheet2D;
use toml;

#[derive(clap::Parser, Debug)]
struct Args {
    #[clap(short, long, default_value = "input.txt")]
    input: PathBuf,
    #[clap(short, long, default_value = "output.txt")]
    output: PathBuf,
    #[clap(short, long, default_value = "config.toml")]
    config: PathBuf,
    #[clap(long)]
    gen_config:Option<PathBuf>,
}

#[derive(Serialize,Deserialize)]
struct CalcOptions {
    left:String,
    right:String,
    operation:String,
    result:String,
}

#[derive(Serialize,Deserialize)]
struct CalcConfig {
    line_offset:Option<usize>,
    column_delimeter:Option<String>,
    calculation:Vec<CalcOptions>,
}

impl Default for CalcConfig {
    fn default() -> Self {

        let op1 = CalcOptions {
            left:"column name pattern 1".to_string(),
            right:"column name pattern 2".to_string(),
            operation: "+".to_string(),
            result:"new column name".to_string()
        };

        let op2 = CalcOptions {
            left:"column name pattern 1".to_string(),
            right:"new column name".to_string(),
            operation: "/".to_string(),
            result:"new column name 2".to_string()
        };

        Self { calculation: vec![op1,op2], line_offset: Some(0), column_delimeter: Some("\t".to_string()) }
    }
}

fn main() -> Result<(),Box<dyn Error>> {
    
    let args = Args::parse();

    if let Some(config_file) = args.gen_config {
        println!("writing config to {:?}",config_file);
        let mut f = File::create(&config_file).expect("cannot create config file");
        f.write_all(toml::to_string(&CalcConfig::default()).unwrap().as_bytes()).expect("cannot write to file");
        return Ok(())
    };

    if !args.config.exists() {
        println!("calculation config not found. You can generate a template by passing --gen-config=config.toml");
        Err(format!("calculation config not found: {:?}",args.config))?
    }

    let mut conf_file = File::open(&args.config)?;
    let mut conf_string = String::new();
    conf_file.read_to_string(&mut conf_string)?;
    let config:CalcConfig = toml::from_str(&conf_string)?;
    
    let mut f = File::open(&args.input)?;

    let mut s = String::new();
    println!("reading file ...");
    f.read_to_string(&mut s)?;

    println!("parsing spreadsheet ...");
    let mut spreadsheet = SpreadSheet2D::from_string(s,&config.column_delimeter.unwrap_or(String::from("\t")),config.line_offset.unwrap_or(0));

    println!("running calculations ...");

    for calc in &config.calculation {
        spreadsheet.column_op(
            &calc.left,
            &calc.operation,
            &calc.right,
            &calc.result
        )?
    }

    println!("writing new spreadsheet to {}",args.output.to_string_lossy());
    let mut new_f = File::create(&args.output)?;
    new_f.write_all(spreadsheet.to_string().as_bytes())?;

    Ok(())
}
