mod log_utils;
mod hyper_tls;

use clap::{App, Arg};
use log::info;
use rdkafka::util::get_rdkafka_version;
use crate::log_utils::*;
use hyper::Client;
use hyper_tls::HttpsConnector;
use futures::{
    future::FutureExt, // for `.fuse()`
    pin_mut,
    select,
};
use tokio::runtime::Runtime;
use tokio::prelude::*;


async fn get_page(url: &str, client: &Client<hyper_tls::client::HttpsConnector<hyper::client::connect::HttpConnector>>) -> Result<String, String> {
    let uri = url.parse().ok().ok_or("couldnt parse url")?;
    let mut r1 = client.get(uri).await.ok().ok_or("problem getting url")?;
    let bs = hyper::body::to_bytes(r1.into_body()).await.ok().ok_or("couldn't get bytes")?;
    let s = std::str::from_utf8(&bs).ok().ok_or("cant convert from utf8")?;
    Ok(String::from(s))
}


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
    info!("using librdkafka version: 0x{:08x}, {}", version_n, version_s);
    info!("cluster: {}", bootstrap_servers);

    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);

    let mut futs = vec![];
    futs.push(Box::pin(get_page("http://confluent.io", &client)));
    futs.push(Box::pin(get_page("https://www.matthowlett.com", &client)));
    futs.push(Box::pin(get_page("https://news.ycombinator.com", &client)));

    loop {
        let mut idx = 0;
        if futs.len() == 0 { break; }
        {
            let ff = futures::future::select_all(futs.iter_mut()).await;
            idx = ff.1;
            match ff.0 {
                Ok(r) => println!("{}", r),
                Err(_) => println!("no result")
            }
        }
        {
            futs.remove(idx);
        }
    }

    Ok(())
}

