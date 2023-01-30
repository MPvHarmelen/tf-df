use clap::Parser;
use itertools::Itertools;
use std::{fs, io};

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

    let sources = serde_json::from_str::<HashMap<String, usize>>(
        fs::read_to_string(args.source_counts)?.as_str(),
    )?
    .into_iter()
    .fold(
        new_hash_map::<_, usize>(),
        |mut sources, (source, count)| {
            *sources.entry(simplify_source(&source)).or_default() += count;
            sources
        },
    );

    // Have to do this manually to keep the ordering :(
    let mut ordered_sources = sources
        .into_iter()
        .sorted_by_key(|(_, count)| std::usize::MAX - *count);

    print!("{{\n");
    if let Some((source, count)) = ordered_sources.next() {
        print!("  \"{}\": {}", source, count)
    };
    ordered_sources.for_each(|(source, count)| print!(",\n  \"{}\": {}", source, count));
    print!("\n}}\n");
    Ok(())
}
