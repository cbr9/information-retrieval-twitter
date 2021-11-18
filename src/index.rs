use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::Tweet;
use anyhow::Result;
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::path::{Path, PathBuf};

#[derive(Default, Serialize, Deserialize)]
pub struct Index<'a> {
	dictionary: Dictionary,
	token_to_id: TokenToID,
	postings_lists: PostingsLists,
}

trait RemovePunctuation {
	fn remove_punctuation(self, punctuation: &HashSet<char>) -> Self;
}

impl RemovePunctuation for String {
	fn remove_punctuation(self, punctuation: &HashSet<char>) -> Self {
		self.chars().filter(|char| !punctuation.contains(char)).collect()
	}
}

#[derive(Default, Serialize, Deserialize)]
struct PostingsList {
	size: usize,
	pointer: u64,
}

impl<'a> Index {

	const PATH: &'a str = "src/data/index.bin";

	pub fn new<T: AsRef<Path>>(path: T) -> Result<Self> {
		let index = std::fs::File::open(&Self::PATH).map_or_else(
			|_| {
				let punctuation: HashSet<char> = HashSet::from_iter(vec![
					'!', '”', '—', '"', '“', '’', '‘', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=', '>',
					'?', '@', '[', '\\', ']', '^', '_', '`', '{', '|', '}', '~',
				]);
				let stopwords_file = std::fs::read_to_string("src/stopwords/english.txt").unwrap();
				let stopwords = stopwords_file
					.lines()
					.map(|line| line.to_string().remove_punctuation(&punctuation))
					.collect::<HashSet<_>>();

				let mut data = ReaderBuilder::new().delimiter(b'\t').from_path(path.as_ref()).unwrap();

				let mut hasher = DefaultHasher::new();
				// INITIALIZE DATA STRUCTURES
				let mut dictionary = HashMap::new();
				let mut token_to_id = HashMap::new();
				let mut postings_lists = HashMap::new();

				for result in data.deserialize() {
					let tweet: Tweet = result.unwrap();
					let Tweet { id: doc_id, body, .. } = tweet;
					let body = body
						.replace("[NEWLINE]", "\n")
						.replace("[TAB]", "\t")
						.to_lowercase()
						.remove_punctuation(&punctuation);

					body.split_whitespace().into_iter().for_each(|token| {
						if !stopwords.contains(token) {
							let term_id = token_to_id.entry(token.to_string()).or_insert_with(|| {
								token.hash(&mut hasher);
								hasher.finish()
							});
							let postings_list = dictionary.entry(token.to_string()).or_insert(PostingsList {
								size: 0,
								pointer: term_id.clone(),
							});

							postings_lists
								.entry(term_id.clone())
								.and_modify(|postings: &mut HashSet<u64>| {
									postings_list.size += 1;
									postings.insert(doc_id.clone());
								})
								.or_insert_with(|| {
									postings_list.size += 1;
									HashSet::from_iter(vec![doc_id.clone()])
								});
						}
					});
				}

				let postings_lists = HashMap::from_iter(postings_lists.iter().map(|(key, value)| {
					let mut as_vec = value.into_iter().cloned().collect::<Vec<_>>();
					as_vec.sort();
					((*key).clone(), as_vec)
				}));

				let index = Index {
					dictionary: Dictionary(dictionary),
					token_to_id: TokenToID(token_to_id),
					postings_lists: PostingsLists(postings_lists),
				};
				index.persist().unwrap();
				index
			},
			|reader| -> Index { bincode::deserialize_from(reader).unwrap() },
		);
		Ok(index)
	}

	fn intersect(arr1: &Vec<u64>, arr2: &Vec<u64>) -> Vec<u64> {
		let mut iter1 = arr1.into_iter();
		let mut iter2 = arr2.into_iter();
		let mut post1 = iter1.next();
		let mut post2 = iter2.next();

		let mut intersection = Vec::new();

		while post1.is_some() && post2.is_some() {
			let p1 = post1.unwrap();
			let p2 = post2.unwrap();
			if p1 < p2 {
				post1 = iter1.next();
			} else if p1 == p2 {
				intersection.push(p1.clone());
				post1 = iter1.next();
				post2 = iter2.next();
			} else {
				post2 = iter2.next();
			}
		}
		intersection
	}

	pub fn query(&self, terms: Vec<&str>, partial_intersection: Option<Vec<u64>>) -> Vec<u64> {
		let mut n_terms = terms.len();

		return if n_terms == 0 {
			Vec::new()
		} else if partial_intersection.is_some() && n_terms > 0 {
			let partial_intersection = partial_intersection.unwrap();
			Self::intersect(&partial_intersection, &self.postings_lists[&self.token_to_id[terms[0]]])
		} else {
			if n_terms == 1 {
				self.postings_lists[&self.token_to_id[terms[0]]].clone()
			} else if n_terms == 2 {
				let post1 = self.postings_lists.get(self.token_to_id.get(terms[0]).unwrap()).unwrap();
				let post2 = self.postings_lists.get(self.token_to_id.get(terms[1]).unwrap()).unwrap();
				Self::intersect(post1, post2)
			} else {
				let (first_two, mut terms) = terms.split_at(2);
				let mut partial_intersection = self.query(first_two.to_vec(), None);
				n_terms -= 2;

				while n_terms > 0 {
					partial_intersection = self.query(vec![terms[0]], Some(partial_intersection));
					n_terms -= 1;
					terms = &terms[1..]
				}
				partial_intersection
			}
		};
	}

	fn persist(&self) -> bincode::Result<()> {
		let file = std::fs::File::create(&Self::PATH)?;
		bincode::serialize_into(file, self)
	}
}

#[derive(Default, Serialize, Deserialize)]
struct PostingsLists(HashMap<u64, Vec<u64>>);

impl Deref for PostingsLists {
	type Target = HashMap<u64, Vec<u64>>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[derive(Default, Serialize, Deserialize)]
struct Dictionary(HashMap<String, PostingsList>);

impl Deref for Dictionary {
	type Target = HashMap<String, PostingsList>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[derive(Default, Serialize, Deserialize)]
struct TokenToID(HashMap<String, u64>);

impl Deref for TokenToID {
	type Target = HashMap<String, u64>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
