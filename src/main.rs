mod crawler;
mod hyper_tls;
mod log_utils;

use crate::log_utils::*;
use bloom;
use clap::{App, Arg};
use futures::{
    future::FutureExt, // for `.fuse()`
    pin_mut,
    select,
};
use hyper::Client;
use hyper_tls::HttpsConnector;
use log::info;
use rdkafka::util::get_rdkafka_version;
use std::cell::RefCell;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let matches = App::new("www crawler")
        .about("Web crawler built with Rust and Kafka")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let bootstrap_servers = matches.value_of("brokers").unwrap();

    let (version_n, version_s) = get_rdkafka_version();
    info!("welcome to rust www crawler!");
    info!(
        "using librdkafka version: 0x{:08x}, {}",
        version_n, version_s
    );
    info!("cluster: {}", bootstrap_servers);

    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);

    let mut positive: u64 = 0;
    let mut negative: u64 = 0;

    let expected_num_items = 100000;

    let false_positive_rate = 0.01;

    let mut filter = RefCell::new(bloom::BloomFilter::with_rate(
        false_positive_rate,
        expected_num_items,
    ));

    let c = crawler::Crawler {
        client: &client,
        filter: &filter,
    };

    let mut futs = vec![];
    futs.push(Box::pin(c.get_page("http://confluent.io")));
    futs.push(Box::pin(c.get_page("https://www.matthowlett.com")));
    futs.push(Box::pin(c.get_page("https://news.ycombinator.com")));
    futs.push(Box::pin(c.get_page("https://nytimes.com")));

    loop {
        let mut idx = 0;
        if futs.len() == 0 {
            break;
        }
        {
            let ff = futures::future::select_all(futs.iter_mut()).await;
            idx = ff.1;
            match ff.0 {
                Ok(r) => {
                    let res = c.process_webpage(r.as_str(), &mut positive, &mut negative);
                    for x in res {
                        println!("{}", x);
                    }
                }
                Err(_) => println!("no result"),
            }
        }
        {
            futs.remove(idx);
        }
    }

    println!("happy: {} sad: {}", positive, negative);

    Ok(())
}
