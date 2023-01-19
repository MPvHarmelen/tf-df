use json;
use rayon::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::fs::read_to_string;
use std::io;
use std::path::Path;
use walkdir::{DirEntry, WalkDir};

use clap::Parser;

/// Calculate term frequency and document frequency of a bunch of Devanagari text files
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory containing the data files
    #[arg(short, long)]
    path: String,
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();
    let path = args.path;

    // If this fails, the code shouldn't compile??
    let splitter = Regex::new(r"\P{Devanagari}+").expect("Illegal regex");

    let (mut term_frequency, mut document_frequency) = WalkDir::new(path)
        .min_depth(1)
        .into_iter()
        .map(|x| x.unwrap()) // panic on errors
        .collect::<Vec<_>>() // get all files
        .into_par_iter()
        .filter(|e| (!e.path().is_dir()) && is_not_hidden(e))
        .map(|x| process_file(x.path(), &splitter))
        .reduce(
            // this is a parallel reduce
            || Ok((HashMap::new(), HashMap::new())),
            |wrapped_left, wrapped_right| {
                let (mut left_tf, mut left_df) = wrapped_left?;
                let (right_tf, right_df) = wrapped_right?;
                right_tf.into_iter().for_each(|(k, v)| {
                    left_tf
                        .entry(k)
                        .and_modify(|counter| *counter += v)
                        .or_insert(1);
                });
                right_df.into_iter().for_each(|(k, v)| {
                    left_df
                        .entry(k)
                        .and_modify(|counter| *counter += v)
                        .or_insert(1);
                });
                Ok((left_tf, left_df))
            },
        )
        .expect("No files found");

    // drop all words that only occur once
    term_frequency = term_frequency
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .collect();

    document_frequency = document_frequency
        .into_iter()
        .filter(|(term, _)| term_frequency.contains_key(term))
        .collect();

    print!("{{ \"term-frequency\": ");
    print!("{}", json::stringify_pretty(term_frequency, 2));
    print!(",\n \"document-frequency\": ");
    print!("{}", json::stringify_pretty(document_frequency, 2));
    println!("}}");

    Ok(())
}

fn process_file(
    path: &Path,
    splitter: &Regex,
) -> Result<(HashMap<String, usize>, HashMap<String, usize>), io::Error> {
    let mut map: HashMap<&str, usize> = HashMap::new();
    let contents = read_to_string(path)?;
    for token in splitter.split(&contents) {
        // https://doc.rust-lang.org/std/collections/struct.HashMap.html#method.entry
        map.entry(token)
            .and_modify(|counter| *counter += 1)
            .or_insert(1);
    }
    let term_frequency: HashMap<String, usize> = map
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
    let document_frequency: HashMap<String, usize> = term_frequency
        .keys()
        .cloned()
        .map(|token| (token, 1))
        .collect();
    Ok((term_frequency, document_frequency))
}

fn is_not_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| entry.depth() == 0 || !s.starts_with("."))
        .unwrap_or(false)
}
