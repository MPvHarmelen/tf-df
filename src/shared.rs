use itertools::Itertools;
use serde::Deserialize;
use std::hash::{BuildHasherDefault, Hash};

pub type HashMap<A, B> = std::collections::HashMap<A, B, BuildHasherDefault<rustc_hash::FxHasher>>;
pub type HashSet<A> = std::collections::HashSet<A, BuildHasherDefault<rustc_hash::FxHasher>>;

pub fn new_hash_map<A, B>() -> HashMap<A, B> {
    rustc_hash::FxHashMap::default()
}

pub fn folder(
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

type DoubleCounts<A> = HashMap<A, (usize, usize)>;

pub fn add_counts<A: Hash + Eq>(
    mut left_counts: DoubleCounts<A>,
    right_counts: DoubleCounts<A>,
) -> DoubleCounts<A> {
    // https://github.com/rust-lang/rfcs/pull/2593
    right_counts.into_iter().for_each(|(token, (tf, df))| {
        let (left_tf, left_df) = left_counts.entry(token).or_default();
        *left_tf += tf;
        *left_df += df;
    });
    left_counts
}

#[derive(Deserialize)]
pub struct Document {
    // #[serde(rename="newsId")]
    // id: String,
    #[serde(rename = "newsSource")]
    pub source: String,

    #[serde(rename = "newsText")]
    pub text: String,
}

pub fn simplify_source<S: AsRef<str>>(sourceref: &S) -> String {
    let mut source = sourceref.as_ref();
    source = source.strip_suffix('/').unwrap_or(source);
    let mut parts = source
        .strip_prefix("http://")
        .unwrap_or_else(|| source.strip_prefix("https://").unwrap_or(source))
        .split('.')
        .map(
            |s|
            // Drop any port parts
            s.split(':').next().unwrap(), // splitting always gives at least one result
        )
        .filter(|s| s != &"www")
        .collect::<Vec<_>>();
    // If the pattern is something.org.np, keep three things.
    let bits = if parts.len() >= 2 && parts[parts.len() - 2].len() <= 3 {
        3
    } else {
        2
    };
    parts.into_iter().rev().take(bits).rev().join(".")
}
