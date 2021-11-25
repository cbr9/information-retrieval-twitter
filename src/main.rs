mod data;
mod index;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use anyhow::Result;
use clap::Parser;
use itertools::Itertools;
use polars::prelude::*;

use crate::index::Index;

#[derive(Debug, Deserialize, Serialize, Default)]
struct Tweet {
	id: u64,
	user_id: String,
	user_name: String,
	body: String,
}

#[derive(Parser)]
struct Opts {
	#[clap(subcommand)]
	command: Command,
}

#[derive(Parser)]
enum Command {
	Query(Query),
}

#[derive(Parser)]
struct Query {
	#[clap(long, required = true)]
	terms: Vec<String>,
}

fn main() -> Result<()> {
	let path = "src/data/twitter-cleaned.csv";
	let index = Index::new(path)?;
	let data = CsvReader::from_path(path)?
		.with_delimiter(b'\t')
		.infer_schema(None)
		.finish()?
		.drop_duplicates(true, None)?;

	let opts: Opts = Opts::parse();
	match opts.command {
		Command::Query(query) => {
			let terms = query.terms.iter().map(|string| string.as_str()).collect_vec();
			index
				.retrieve_documents(terms, &data)?
				.into_iter()
				.for_each(|body| println!("{}", body));
		}
	}
	Ok(())
}
