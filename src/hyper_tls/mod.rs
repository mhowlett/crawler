//! # hyper-tls
//!
//! An HTTPS connector to be used with [hyper][].
//!
//! [hyper]: https://hyper.rs
//!
//! ## Example
//!
//! ```no_run
//! use hyper_tls::HttpsConnector;
//! use hyper::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>>{
//!     let https = HttpsConnector::new();
//!     let client = Client::builder().build::<_, hyper::Body>(https);
//!
//!     let res = client.get("https://hyper.rs".parse()?).await?;
//!     assert_eq!(res.status(), 200);
//!     Ok(())
//! }
//! ```

pub mod client;
mod stream;

pub extern crate native_tls;

pub use client::{HttpsConnecting, HttpsConnector};
pub use stream::{MaybeHttpsStream, TlsStream};
