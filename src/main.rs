mod data;
mod index;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use anyhow::Result;
use clap::Parser;
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

fn retrieve_documents(query_results: Vec<u64>, data: &DataFrame) -> Result<()> {
	let results = Series::from_iter(query_results);
	let mask = (*data.column("id")?).is_in(&results)?;
	let docs = data.filter(&mask)?;
	docs.column("body")?.utf8()?.into_no_null_iter().for_each(|body| {
		println!("{}", body);
	});
	Ok(())
}

fn main() -> Result<()> {
	// let opts: Opts = Opts::parse();
	let path = "src/data/twitter-cleaned.csv";
	let index = Index::new(path)?;
	let data = CsvReader::from_path(path)?
		.with_delimiter(b'\t')
		.infer_schema(None)
		.finish()?
		.drop_duplicates(true, None)?;

	let query = index.query(vec!["*nu*", "covid"]);
	retrieve_documents(query, &data)?;
	println!("{}", "-------------------------------------");

	// match opts.command {
	// 	Command::Query(query) => {
	// 		let terms = query.terms.iter().map(|string| string.as_str()).collect_vec();
	// 		let results = Series::from_iter(index.query(terms, None));
	// 		let mask = (*data.column("id")?).is_in(&results)?;
	// 		let docs = data.filter(&mask)?;
	// 		docs.column("body")?.utf8()?.into_no_null_iter().for_each(|body| {
	// 			println!("{}", body);
	// 		});
	// 	}
	// }

	Ok(())
}
