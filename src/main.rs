use json;
use regex::Regex;
use std::fs::read_to_string;
use std::io;
use std::iter::Map;
use std::path::Path;
use std::{collections::HashMap, io::Write};
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

    let term_frequency = WalkDir::new(path)
        .min_depth(1)
        .into_iter()
        .filter_map(|v| v.ok())
        .filter(|e| (!e.path().is_dir()) && is_not_hidden(e))
        .map(|x| process_file(x.path(), &splitter))
        .reduce(|wrapped_left, wrapped_right| {
            let mut left = wrapped_left?;
            wrapped_right?.into_iter().for_each(|(k, v)| {
                left.entry(k)
                    .and_modify(|counter| *counter += v)
                    .or_insert(1);
            });
            Ok(left)
        })
        .expect("No files found")?;

    let string = json::stringify_pretty(term_frequency, 2);
    println!("{}", string);

    Ok(())
}

fn process_file(path: &Path, splitter: &Regex) -> Result<HashMap<String, usize>, io::Error> {
    let mut map: HashMap<&str, usize> = HashMap::new();
    let contents = read_to_string(path)?;
    for token in splitter.split(&contents) {
        // https://doc.rust-lang.org/std/collections/struct.HashMap.html#method.entry
        map.entry(token)
            .and_modify(|counter| *counter += 1)
            .or_insert(1);
    }
    Ok(map
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect())
}

fn is_not_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| entry.depth() == 0 || !s.starts_with("."))
        .unwrap_or(false)
}
