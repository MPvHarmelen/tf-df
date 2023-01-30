use clap::Parser;
use rayon::prelude::*;
use std::fs;
use std::io;
use walkdir::WalkDir;

#[path = "../shared.rs"]
mod shared;
use shared::*;

/// Calculate term frequency and document frequency of a bunch of Devanagari text files
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory containing the data files (in JSON-format)
    #[arg(short, long)]
    path: String,

    /// File containing the counts per source
    #[arg(short, long)]
    source_counts: String,

    /// The minimum number of times a source must be present in the data to use documents from it
    #[arg(long, default_value_t = 0)]
    min_src_freq: usize,

    /// The minimum number of times a type must be present in the data to be considered correct
    #[arg(long, default_value_t = 0)]
    min_tf: usize,

    /// The minimum number of documents a type must appear in to be considered correct
    #[arg(long, default_value_t = 0)]
    min_df: usize,
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();
    let sources: HashSet<String> = serde_json::from_str::<HashMap<String, usize>>(
        fs::read_to_string(args.source_counts)?.as_str(),
    )?
    .into_iter()
    .filter_map(|(source, count)| {
        if count >= args.min_src_freq {
            Some(source)
        } else {
            None
        }
    })
    .collect();

    let mut word_counts = WalkDir::new(args.path)
        .into_iter()
        .collect::<Vec<_>>() // get all files
        .into_par_iter()
        .map(|x| x.unwrap().into_path())
        .filter(|p| (!p.is_dir()))
        .try_fold(new_hash_map, |mut counts, path| {
            let contents = fs::read_to_string(&path)?;
            counts = serde_json::from_str::<Vec<Document>>(&contents)
                .unwrap()
                .into_iter()
                .filter(|d| {
                    // I don't know whether the passed sources have been simplified,
                    // so let's just check both 🤷
                    sources.contains(&d.source)
                        || sources.contains(simplify_source(&d.source).as_str())
                })
                .fold(counts, |counts, doc| folder(counts, doc.text));
            Ok::<_, io::Error>(counts)
        })
        .try_reduce(new_hash_map, |left_counts, right_counts| {
            Ok(add_counts(left_counts, right_counts))
        })?;

    if args.min_tf > 0 || args.min_df > 0 {
        word_counts.retain(|_, (tf, df)| *tf >= args.min_tf && *df >= args.min_df)
    }
    print!(
        "{}",
        serde_json::to_string_pretty(&word_counts).expect("Serializing json failed")
    );

    Ok(())
}
