mod index;
mod data;

use std::fmt::Debug;
use serde::{Deserialize, Serialize};

use anyhow::Result;
use polars::prelude::*;

use crate::index::Index;


#[derive(Debug, Deserialize, Serialize, Default)]
struct Tweet {
	id: u64,
	user_id: String,
	user_name: String,
	body: String,
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
