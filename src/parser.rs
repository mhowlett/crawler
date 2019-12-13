use std::collections::HashSet;
use std::str;
use std::vec::Vec;

use hyper;
use select::document::Document;
use select::predicate::Name;

use lazy_static;

lazy_static::lazy_static! {
    static ref POSITIVE_SET: HashSet<&'static str> = vec!["happy","cheerful","contented","delighted","ecstatic","elated","glad","joyful","joyous","jubilant","lively"].into_iter().collect();
    static ref NEGATIVE_SET: HashSet<&'static str> = vec!["sad","bitter","dismal","heartbroken","melancholy","mournful","pessimistic","somber","sorrowful","sorry","wistful"].into_iter().collect();
}

pub fn process_webpage(html: &str, positive: &mut u64, negative: &mut u64) -> Vec<String> {
    sentiment_analysis(html, positive, negative);
    return parse_webpage(html);
}

fn parse_webpage(html: &str) -> Vec<String> {
    let mut url_list = vec![];
    let html = Document::from(html);

    let urls = html.find(Name("a")).filter_map(|n| n.attr("href"));

    for url in urls {
        if is_valid_url(url) {
            url_list.push(String::from(url));
        }
    }

    return url_list;
}

fn is_valid_url(url: &str) -> bool {
    return url.starts_with("http");
}

fn sentiment_analysis(input: &str, positive: &mut u64, negative: &mut u64) {
    let mut happy_count = 0;
    let mut sad_count = 0;
    input.split_whitespace().for_each(|s| {
        if POSITIVE_SET.contains(s) {
            *positive += 1;
        }
        if NEGATIVE_SET.contains(s) {
            *positive += 1;
        }
    });
}
