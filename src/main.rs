mod crawler;
mod hyper_tls;
mod log_utils;

extern crate log;
extern crate clap;
extern crate futures;
extern crate rdkafka;
extern crate rdkafka_sys;

use clap::{App, Arg};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::util::get_rdkafka_version;
use futures::compat::Stream01CompatExt;
use crate::futures::StreamExt;

use log::{info, warn};
use crate::log_utils::*;
use hyper::Client;
use hyper_tls::HttpsConnector;
// use futures::{
//     future::FutureExt, // for `.fuse()`
//     pin_mut,
//     select,
// };
use tokio::runtime::Runtime;
use tokio::prelude::*;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::log_utils::*;
use bloom;

use std::cell::RefCell;


const URL_TOPIC: &str = "urls";


// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    // fn commit_callback(
    //     &self,
    //     result: KafkaResult<()>,
    //     _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList,
    // ) {
    //     info!("Committing offsets: {:?}", result);
    // }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

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
    info!("bootstrap.servers: {}", bootstrap_servers);

    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", "url_cg")
        .set("bootstrap.servers", bootstrap_servers)
        .set("enable.partition.eof", "true")
        .set("enable.auto.commit", "true")
        // .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[URL_TOPIC])
        .expect("Can't subscribe to specified topics");

    // seed with hacker news.
    producer.send(FutureRecord::to(URL_TOPIC).payload("").key("https://news.ycombinator.com"), 0);

    let mut message_stream = consumer.start().compat();

    const MAX_INFLIGHT: usize = 10;

    let mut futs = vec![];
    
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

/*
    let mut futs = vec![];
    futs.push(Box::pin(c.get_page("http://confluent.io")));
    futs.push(Box::pin(c.get_page("https://www.matthowlett.com")));
    futs.push(Box::pin(c.get_page("https://news.ycombinator.com")));
    futs.push(Box::pin(c.get_page("https://nytimes.com")));
*/

    loop {
        // if we don't have max requests in flight, or we haven't reached partition eof (currently assume 1 partition only: todo fix this),
        // then keep consuming urls to read and add get_page tasks corresponding to them. 
        // 
        // if either of these conditions fails, then switch to waiting on get page completions.
        // 
        // this is a bit flawed.
        loop {
            let mut eof = false;
            let mut url = "";

            if let Some(message) = message_stream.next().await {
                match message {
                    Err(_) => warn!("Error while reading from stream."),
                    Ok(Err(e)) => {
                        if let rdkafka::error::KafkaError::PartitionEOF(_) = e {
                            eof = true;
                            info!("eof");
                        } else {
                            warn!("consume error");
                        }
                    },
                    Ok(Ok(msg)) => {
                        let aaa = msg.key();
                        let aa = match aaa {
                            None => {
                                warn!("no key");
                            },
                            Some(bs) => {
                                let b = std::str::from_utf8(&bs)?;
                                url = b.clone();
                            }
                        };
                    }
                };
            } else {
                warn!("unexpected, I think");
            }

            if eof || futs.len() >= MAX_INFLIGHT {
                break;
            }

            futs.push(Box::pin(c.get_page(url)));
        }

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
                    // producer.send(FutureRecord::to(URL_TOPIC).payload("").key("https://news.ycombinator.com"), 0);
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
