use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;
use rayon::prelude::ParallelString;
use rayon::iter::ParallelIterator;
use regex::Regex;
use crate::{Tweet};

lazy_static! {
	static ref TWEET_PATTERN: Regex = Regex::new(r"(?P<id>\d+)\t(?P<user_id>@.+)\t(?P<user_name>.+)\t(?P<body>.+)").unwrap();
}

#[allow(dead_code)]
fn clean_data() -> anyhow::Result<()> {
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
