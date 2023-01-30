use clap::Parser;
use itertools::Itertools;
use std::{collections::BTreeMap, fs, io};

#[path = "../shared.rs"]
mod shared;
// use itertools::Itertools;
use shared::*;

/// Clean source names and add their counts
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File containing the counts per source
    #[arg(short, long)]
    source_counts: String,
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();

    let sources = serde_json::from_str::<BTreeMap<String, usize>>(
        fs::read_to_string(args.source_counts)?.as_str(),
    )?
    .into_iter()
    .fold(
        BTreeMap::<_, usize>::new(),
        |mut sources, (source, count)| {
            *sources.entry(simplify_source(&source)).or_default() += count;
            sources
        },
    );
    // Serde doesn't keep the ordering :(
    print!("{{\n");
    sources
        .into_iter()
        .sorted_by_key(|(_, count)| std::usize::MAX - *count)
        .for_each(|(source, count)| print!("  \"{}\": {},\n", source, count));
    print!("}}\n");
    Ok(())
}
