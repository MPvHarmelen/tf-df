use rayon::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::fs::read_to_string;
use std::io;
use std::path::Path;
use walkdir::WalkDir;

use clap::Parser;

/// Calculate term frequency and document frequency of a bunch of Devanagari text files
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory containing the data files
    #[arg(short, long)]
    path: String,

    /// Minimum term frequency
    #[arg(short, long, default_value_t = 0)]
    min_frequency: usize,
}

fn new_hash_map<A, B>() -> HashMap<A, B> {
    HashMap::new()
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();
    let path = args.path;

    // If this fails, the code shouldn't compile??
    let splitter = Regex::new(r"\P{Devanagari}+").expect("Illegal regex");

    // let (words, mut counts) =

    let mut counts = WalkDir::new(path)
        .min_depth(1)
        .into_iter()
        .collect::<Vec<_>>() // get all files
        .into_par_iter()
        .map(|x| x.unwrap().into_path()) // panic on errors
        .filter(|p| (!p.is_dir()))
        .try_fold(new_hash_map, |mut counts, p| {
            process_file(&p, &splitter)?
                .into_iter()
                .for_each(|(term, count)| {
                    let (tf, df): &mut (usize, usize) = counts.entry(term).or_default();
                    *tf += count;
                    *df += 1;
                });
            Ok::<_, io::Error>(counts)
        })
        .try_reduce(new_hash_map, |mut left_counts, right_counts| {
            right_counts.into_iter().for_each(|(token, (tf, df))| {
                let (left_tf, left_df) = left_counts.entry(token).or_default();
                *left_tf += tf;
                *left_df += df;
            });
            Ok(left_counts)
        })?;

    if args.min_frequency > 0 {
        counts.retain(|_, (tf, _)| *tf > args.min_frequency);
    }

    print!(
        "{}",
        serde_json::to_string_pretty(&counts).expect("Serializing json failed")
    );

    Ok(())
}

fn process_file(path: &Path, splitter: &Regex) -> Result<HashMap<String, usize>, io::Error> {
    let mut map = new_hash_map();
    let contents = read_to_string(path)?;
    splitter
        .split(&contents)
        .for_each(|token| *map.entry(token.to_string()).or_default() += 1);
    Ok(map)
}
