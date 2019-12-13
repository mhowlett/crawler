mod log_utils;

use clap::{App, Arg};
use log::info;
use rdkafka::util::get_rdkafka_version;
use crate::log_utils::*;


#[tokio::main]
async fn main() {
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
}
