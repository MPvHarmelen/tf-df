use rayon::prelude::*;
use regex::Regex;
use std::fs::read_to_string;
use std::io;
use std::path::Path;
use std::{collections::HashMap, hash::BuildHasherDefault};
use walkdir::{DirEntry, WalkDir};

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

type Counts<A = String> = HashMap<A, usize, BuildHasherDefault<rustc_hash::FxHasher>>;

fn new_counts<A>() -> Counts<A> {
    rustc_hash::FxHashMap::default()
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
            || Ok((new_counts(), new_counts())),
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
    if args.min_frequency > 0 {
        term_frequency.retain(|_, count|*count > args.min_frequency);
        document_frequency.retain(|term, _| term_frequency.contains_key(term));
    }

    print!("{{ \"term_frequency\": ");
    print!(
        "{}",
        serde_json::to_string_pretty(&term_frequency).expect("Serializing json failed")
    );
    print!(",\n \"document_frequency\": ");
    print!(
        "{}",
        serde_json::to_string_pretty(&document_frequency).expect("Serializing json failed")
    );
    println!("}}");

    Ok(())
}

fn process_file(path: &Path, splitter: &Regex) -> Result<(Counts, Counts), io::Error> {
    let mut map = new_counts();
    let contents = read_to_string(path)?;
    for token in splitter.split(&contents) {
        // https://doc.rust-lang.org/std/collections/struct.HashMap.html#method.entry
        map.entry(token)
            .and_modify(|counter| *counter += 1)
            .or_insert(1);
    }
    let term_frequency: Counts = map
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
    let document_frequency: Counts = term_frequency
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
