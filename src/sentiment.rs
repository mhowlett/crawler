use std::collections::HashSet;

use lazy_static;

lazy_static::lazy_static! {
    static ref HAPPYSET: HashSet<&'static str> = vec!["happy"].into_iter().collect();
    static ref SADSET: HashSet<&'static str> = vec!["sad"].into_iter().collect();
}

pub fn number_happy_and_sad(input: &str) -> (i32, i32) {
    let mut happy_count = 0;
    let mut sad_count = 0;
    input.split_whitespace().for_each(|s| {
        if HAPPYSET.contains(s) {
            happy_count += 1;
        }
        if SADSET.contains(s) {
            sad_count += 1;
        }
    });
    return (happy_count, sad_count);
}
