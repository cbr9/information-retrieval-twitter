mod index;

use std::fmt::Debug;

use lazy_static::lazy_static;
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use polars::prelude::*;

use crate::index::Index;

lazy_static! {
	static ref TWEET_PATTERN: Regex = Regex::new(r"(?P<id>\d+)\t(?P<user_id>@.+)\t(?P<user_name>.+)\t(?P<body>.+)").unwrap();
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct Tweet {
	id: u64,
	user_id: String,
	user_name: String,
	body: String,
}

#[allow(dead_code)]
fn fix_csv() -> Result<()> {
	let original = "src/data/twitter.csv";
	let clean = "src/data/twitter-cleaned.csv";

	let wrt = csv::WriterBuilder::new().delimiter(b'\t').from_path(&clean)?;
	let wrt = Arc::new(Mutex::new(wrt));

	std::fs::read_to_string(&original)?.par_lines().for_each(|line| {
		let tweet = TWEET_PATTERN.captures(line).map(|groups| {
			let id = groups.name("id").map(|id| id.as_str().parse::<u64>().unwrap()).unwrap();
			let user_id = groups.name("user_id").map(|user_id| user_id.as_str().to_string()).unwrap();
			let user_name = groups.name("user_name").map(|name| name.as_str().to_string()).unwrap();
			let body = groups.name("body").map(|body| body.as_str().trim().replace("\t", " ")).unwrap();

			Tweet {
				id,
				user_id,
				user_name,
				body,
			}
		});

		if let Some(tweet) = tweet {
			let wrt = Arc::clone(&wrt);
			wrt.lock().unwrap().serialize(tweet).unwrap();
		}
	});
	wrt.lock().unwrap().flush()?;
	Ok(())
}

fn main() -> Result<()> {
	// fix_csv()?;
	let path = "src/data/twitter-cleaned.csv";
	let index = Index::new(path)?;
	let data = CsvReader::from_path(path)?
		.with_delimiter(b'\t')
		.infer_schema(None)
		.finish()?
		.drop_duplicates(true, None)?;

	let results = Series::from_iter(index.query(vec!["vaccine", "covid", "malaria"], None));
	let mask = (*data.column("id")?).is_in(&results)?;
	let docs = data.filter(&mask)?;

	docs.column("body")?.utf8()?.into_no_null_iter().for_each(|body| {
		println!("{}", body);
	});

	Ok(())
}
