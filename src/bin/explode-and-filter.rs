use clap::Parser;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::read_to_string;
use std::io;
use std::path::Path;

/// Given a list of words and a list of suffixes, generate all possible
/// combinations and filter them by another given list of words
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File containing the newline-separated words to use for generation
    #[arg(short, long)]
    roots: String,

    /// File containing the newline-separated suffixes for generation
    #[arg(short, long)]
    suffixes: String,

    /// File containing the newline-separated words to use for filtering
    #[arg(short, long)]
    filter: String,
}

fn read_lines<P, C>(path: P) -> Result<C, io::Error>
where
    P: AsRef<Path>,
    C: std::iter::FromIterator<std::string::String>,
{
    Ok(read_to_string(path)?
        .split('\n')
        .map(|s| s.to_string())
        .collect())
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();

    let suffixes: Vec<_> = read_lines(args.suffixes)?;
    let filter: HashSet<_> = read_lines(args.filter)?;
    read_to_string(args.roots)?
        .split('\n')
        .par_bridge()
        .for_each(|root| {
            suffixes.iter().for_each(|suffix| {
                let word = format!("{}{}", root, suffix);
                if filter.contains(&word) {
                    println!("{}", word)
                }
            })
        });
    Ok(())
}
