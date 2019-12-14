use std::collections::HashSet;
use std::str;
use std::vec::Vec;

use crate::hyper_tls::HttpsConnector;
use crate::log_utils::*;
use bloom;
use clap::{App, Arg};
use futures::{
    future::FutureExt, // for `.fuse()`
    pin_mut,
    select,
};
use hyper::Client;
use lazy_static;
use log::info;
use rdkafka::util::get_rdkafka_version;
use select::document::Document;
use select::predicate::Name;
use std::cell::RefCell;
use std::cell::RefMut;
use tokio::prelude::*;
use tokio::runtime::Runtime;

lazy_static::lazy_static! {
    static ref POSITIVE_SET: HashSet<&'static str> = vec!["happy","cheerful","contented","delighted","ecstatic","elated","glad","joyful","joyous","jubilant","lively"].into_iter().collect();
    static ref NEGATIVE_SET: HashSet<&'static str> = vec!["sad","bitter","dismal","heartbroken","melancholy","mournful","pessimistic","somber","sorrowful","sorry","wistful"].into_iter().collect();
}

pub struct Crawler<'crawler> {
    pub filter: &'crawler RefCell<bloom::BloomFilter>,
    pub client: &'crawler Client<HttpsConnector<hyper::client::connect::HttpConnector>>,
}

impl Crawler<'_> {
    pub async fn get_page(&self, url: &str) -> Result<String, String> {
        if let Ok(mut filter) = self.filter.try_borrow_mut() {
            filter.insert(&String::from(url));
        } else {
            println!("can't get filter");
        }

        let uri = url.parse().ok().ok_or("couldnt parse url")?;
        let mut r1 = self
            .client
            .get(uri)
            .await
            .ok()
            .ok_or("problem getting url")?;
        let bs = hyper::body::to_bytes(r1.into_body())
            .await
            .ok()
            .ok_or("couldn't get bytes")?;
        let s = std::str::from_utf8(&bs)
            .ok()
            .ok_or("cant convert from utf8")?;
        Ok(String::from(s))
    }

    pub fn process_webpage(
        &self,
        html: &str,
        positive: &mut u64,
        negative: &mut u64,
    ) -> Vec<String> {
        sentiment_analysis(html, positive, negative);
        return self.parse_webpage(html);
    }

    fn parse_webpage(&self, html: &str) -> Vec<String> {
        let mut url_list = vec![];
        let html = Document::from(html);

        let urls = html.find(Name("a")).filter_map(|n| n.attr("href"));

        for url in urls {
            if is_valid_url(url) {
                if let Ok(filter) = self.filter.try_borrow() {
                    if !filter.contains(&String::from(url)) {
                        url_list.push(String::from(url));
                    }
                } else {
                    println!("can't get filter");
                }
            }
        }

        return url_list;
    }
}

pub fn is_valid_url(url: &str) -> bool {
    return url.starts_with("http");
}

pub fn sentiment_analysis(input: &str, positive: &mut u64, negative: &mut u64) {
    input.split_whitespace().for_each(|s| {
        if POSITIVE_SET.contains(s) {
            *positive += 1;
        }
        if NEGATIVE_SET.contains(s) {
            *negative += 1;
        }
    });
}
