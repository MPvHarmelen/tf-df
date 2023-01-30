use clap::Parser;
use flate2::read::GzDecoder;
use rayon::prelude::*;
use std::fs::{self, File};
use std::io;
use std::thread;
use tar::Archive;
use walkdir::WalkDir;
#[path = "../shared.rs"]
mod shared;
use shared::*;

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
    let counts;

    if path.ends_with(".tgz") {
        // Make one thread fewer because we already have the unpacking thread
        let num_threads = num_cpus::get() - 1;

        let (mut tx, rx) = spmc::channel();
        let unpack_thread = thread::spawn(move || {
            for entry in Archive::new(GzDecoder::new(File::open(path)?)).entries()? {
                tx.send(Some(io::read_to_string(entry?)?)).unwrap();
            }
            { 0..num_threads }.for_each(|_ignored| tx.send(None).unwrap());
            Ok::<_, io::Error>(())
        });
        let threads: Vec<_> = { 0..num_threads }
            .map(|_i| {
                let receiver = rx.clone();
                thread::spawn(move || {
                    let mut counts = new_hash_map();
                    loop {
                        match receiver.recv().unwrap() {
                            None => return counts,
                            Some(contents) => counts = folder(counts, contents),
                        }
                    }
                })
            })
            .collect();
        unpack_thread.join().unwrap()?;
        counts = threads.into_iter().fold(new_hash_map(), |counts, thread| {
            add_counts(counts, thread.join().unwrap())
        });
    } else {
        counts = WalkDir::new(path)
            .into_iter()
            .collect::<Vec<_>>() // get all files
            .into_par_iter()
            .map(|x| x.unwrap().into_path())
            .filter(|p| (!p.is_dir()))
            .try_fold(new_hash_map, |mut counts, path| {
                let contents = fs::read_to_string(&path)?;
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
            .try_reduce(new_hash_map, |left, right| Ok(add_counts(left, right)))?;
    }

    print!(
        "{}",
        serde_json::to_string_pretty(&counts).expect("Serializing json failed")
    );

    Ok(())
}
