use clap::Parser;
use rayon::prelude::*;
use serde::Deserialize;
use std::fs::read_to_string;
use std::hash::BuildHasherDefault;
use std::io;
use walkdir::WalkDir;

#[derive(Deserialize)]
struct Document {
    // #[serde(rename="newsId")]
    // id: String,

    // #[serde(rename="newsSource")]
    // source: String,

    #[serde(rename="newsText")]
    text: String,
}

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

type HashMap<A, B> = std::collections::HashMap<A, B, BuildHasherDefault<rustc_hash::FxHasher>>;

fn new_hash_map<A, B>() -> HashMap<A, B> {
    rustc_hash::FxHashMap::default()
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();
    let path = args.path;

    let mut counts = WalkDir::new(path)
        .into_iter()
        .collect::<Vec<_>>() // get all files
        .into_par_iter()
        .map(|x| x.unwrap().into_path())
        .filter(|p| (!p.is_dir()))
        .try_fold(new_hash_map, |mut counts, path| {
            let contents = read_to_string(&path)?;
            if path.ends_with(".json") {
                counts = serde_json::from_str::<Vec<Document>>(&contents)
                    .unwrap()
                    .into_iter()
                    .fold(counts, |counts, doc| folder(counts, doc.text))
            } else {
                counts = folder(counts, contents)
            }
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

fn folder(
    mut counts: HashMap<String, (usize, usize)>,
    contents: String,
) -> HashMap<String, (usize, usize)> {
    let mut map: HashMap<_, usize> = new_hash_map();
    let last_word = contents.chars().fold(String::new(), |mut partial, ch| {
        // If the character is inside the Devanagari range, we want
        // to push it onto the current string.
        // https://unicode-table.com/en/blocks/devanagari/
        if ch >= '\u{0900}' && ch <= '\u{097F}' {
            partial.push(ch);
            partial
        } else if partial.len() > 0 {
            // otherwise, we want to save the string (if it isn't empty)
            *map.entry(partial).or_default() += 1;
            String::new()
        } else {
            // otherwise just keep this empty string for the next
            // character
            partial
        }
    });

    if last_word.len() > 0 {
        *map.entry(last_word).or_default() += 1;
    }

    map.into_iter().for_each(|(term, count)| {
        let (tf, df): &mut (usize, usize) = counts.entry(term).or_default();
        *tf += count;
        *df += 1;
    });
    counts
}
