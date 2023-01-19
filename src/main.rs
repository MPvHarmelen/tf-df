use regex::Regex;
use std::collections::HashMap;
use std::fs::read_to_string;
use std::io;
use std::iter::Map;
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

    WalkDir::new(path)
        .into_iter()
        .filter_entry(|e| is_not_hidden(e))
        .filter_map(|v| v.ok())
        .for_each(|x| println!("{}", x.path().display()));

    // If this fails, the code shouldn't compile??
    let splitter = Regex::new(r"\P{Devanagari}+").expect("Illegal regex");

    // let input = File::open(path)?;
    // let buffered = BufReader::new(input);

    // for line in buffered.lines() {
    //     println!("{}", line?);
    // }

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
