use std::borrow::{Borrow, BorrowMut};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash;
use std::hash::{Hash, Hasher};

use crate::Tweet;
use anyhow::Result;
use csv::ReaderBuilder;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::ops::Deref;
use std::path::Path;

lazy_static! {
	static ref WILDCARD: char = '*';
}

struct PostingsList {
	size: usize,
	pointer: u64,
}

pub struct Index {
	dictionary: HashMap<String, PostingsList>,
	token_to_id: HashMap<String, u64>,
	postings_lists: HashMap<u64, Vec<u64>>,
	kgram_index: HashMap<String, Vec<u64>>,
}

trait Kgrams {
	fn kgrams(&self, k: usize) -> Vec<String>;
}

impl Kgrams for Vec<char> {
	fn kgrams(&self, k: usize) -> Vec<String> {
		self.windows(k).map(|window| String::from_iter(window)).collect_vec()
	}
}

trait StringExtension {
	fn remove_punctuation(self, punctuation: &HashSet<char>) -> String;
	fn augment_chars(&self) -> Vec<char>;
	fn augment_chars_right(&self) -> Vec<char>;
	fn augment_chars_left(&self) -> Vec<char>;
}

impl<T: ToString> StringExtension for T {
	fn remove_punctuation(self, punctuation: &HashSet<char>) -> String {
		self.to_string().chars().filter(|char| !punctuation.contains(char)).collect()
	}
	fn augment_chars(&self) -> Vec<char> {
		format!("${}$", self.to_string()).chars().collect_vec()
	}
	fn augment_chars_right(&self) -> Vec<char> {
		format!("{}$", self.to_string()).chars().collect_vec()
	}

	fn augment_chars_left(&self) -> Vec<char> {
		format!("${}", self.to_string()).chars().collect_vec()
	}
}

impl<'a> Index {
	const K: usize = 3; // parameter to control the size of the k-grams in the k-gram index
	const LIMIT: usize = 200000;

	pub fn new<T: AsRef<Path>>(path: T) -> Result<Self> {
		let punctuation: HashSet<char> = HashSet::from_iter(vec![
			'!', '”', '—', '"', '“', '’', '‘', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '[',
			'\\', ']', '^', '_', '`', '{', '|', '}', '~',
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
		let mut kgram_index = HashMap::new();

		for (i, result) in data.deserialize().enumerate() {
			if i == Self::LIMIT {
				break;
			}
			let tweet: Tweet = result.unwrap();
			let Tweet { id: doc_id, body, .. } = tweet;
			let body = body
				.replace("[NEWLINE]", "\n")
				.replace("[TAB]", "\t")
				.to_lowercase()
				.remove_punctuation(&punctuation);

			body.split_whitespace().into_iter().for_each(|token| {
				if !stopwords.contains(token) {
					let token = token.to_string();
					let term_id = token_to_id.entry(token.clone()).or_insert_with(|| {
						token.hash(&mut hasher);
						hasher.finish()
					});

					let postings_list = dictionary.entry(token.clone()).or_insert(PostingsList {
						size: 0,
						pointer: term_id.clone(),
					});

					token.augment_chars().kgrams(Self::K).iter().for_each(|kgram| {
						kgram_index
							.entry(kgram.clone())
							.and_modify(|e: &mut HashSet<u64>| {
								e.insert(*term_id);
							})
							.or_insert(HashSet::from_iter([*term_id]));
					});

					postings_lists
						.entry(*term_id)
						.and_modify(|postings: &mut HashSet<u64>| {
							postings_list.size += 1;
							postings.insert(doc_id);
						})
						.or_insert_with(|| {
							postings_list.size += 1;
							HashSet::from_iter(vec![doc_id])
						});
				}
			});
		}

		let postings_lists = HashMap::from_iter(postings_lists.into_iter().map(|(key, value)| {
			let mut as_vec = value.into_iter().collect::<Vec<_>>();
			as_vec.sort();
			(key, as_vec)
		}));

		let kgram_index = HashMap::from_iter(kgram_index.into_iter().map(|(key, value)| {
			let mut as_vec = value.into_iter().collect::<Vec<_>>();
			as_vec.sort();
			(key, as_vec)
		}));

		let index = Index {
			dictionary,
			token_to_id,
			postings_lists,
			kgram_index,
		};
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
				intersection.push(*p1);
				post1 = iter1.next();
				post2 = iter2.next();
			} else {
				post2 = iter2.next();
			}
		}
		intersection
	}

	pub fn handle_wildcard<T: AsRef<str>>(&self, mut terms: Vec<T>) -> (Vec<u64>, Vec<u64>) {
		let mut new_terms = HashSet::new();
		let mut doc_ids = HashSet::new();
		for term in terms.iter() {
			let term = term.as_ref();
			let mut kgrams = Vec::new();
			let mut chars = term.chars();
			if chars.next() == Some(*WILDCARD) && chars.last() == Some(*WILDCARD) {
				// if the query term is surrounded by wildcard symbols
				kgrams = term.trim_matches(*WILDCARD).chars().collect_vec().kgrams(Self::K);
			} else {
				match term.split_once(*WILDCARD) {
					None => {
						// if there is no wildcard symbol
						// add the term id to the list of term ids
						// this id will be processed by Index::query
						let id = *self.token_to_id.get(term).unwrap();
						new_terms.insert(id);
					}
					Some((first, second)) => {
						// if there is a wildcard symbol at the beginning, end, or middle of the term
						kgrams.extend(first.augment_chars_left().kgrams(Self::K)); // get the kgrams for the part left of the wildcard and prefix it with a dollar sign
						kgrams.extend(second.augment_chars_right().kgrams(Self::K)); // get the kgrams for the part right of the wildcard and suffix it with a dollar sign
					}
				};
			}

			// get the term IDs associated with the retrieved kgrams
			let mut new_terms: HashSet<&u64> = HashSet::from_iter(&mut self.kgram_index[&kgrams[0]].iter());
			for kgram in kgrams.into_iter().skip(1) {
				let set = HashSet::from_iter(&mut self.kgram_index[&kgram].iter());
				new_terms = new_terms.intersection(&set).copied().collect();
			}
			// get the doc ids associated with the retrieved term ids, in an OR fashion
			for term in new_terms {
				doc_ids.extend(self.postings_lists[&term].clone());
			}
		}
		let mut doc_ids = Vec::from_iter(doc_ids);
		doc_ids.sort();
		let new_terms = Vec::from_iter(new_terms);
		(new_terms, doc_ids)
	}

	pub fn query<T: AsRef<str>>(&self, terms: Vec<T>) -> Vec<u64> {
		let (mut term_ids, mut doc_ids) = self.handle_wildcard(terms);

		return if term_ids.is_empty() {
			doc_ids
		} else {
			if !doc_ids.is_empty() {
				while let Some((first, _)) = term_ids.split_first() {
					// while there are still elements in the list of term ids
					// intersect the document ids of the wildcard token in the query
					// with the doc ids of each of the other terms in the query
					doc_ids = Self::intersect(&doc_ids, &self.postings_lists[first]);
					term_ids.remove(0);
				}
				doc_ids
			} else {
				let (first_two, mut term_ids) = term_ids.split_at(2);
				let post1 = &self.postings_lists[&first_two[0]];
				let post2 = &self.postings_lists[&first_two[1]];
				let mut new_doc_ids = Self::intersect(post1, post2);

				while let Some((first, _)) = term_ids.split_first() {
					// while there are still elements in the list of term ids
					let arr1 = &self.postings_lists[first];
					new_doc_ids = Self::intersect(&new_doc_ids, arr1);
					term_ids = &term_ids[1..];
				}
				new_doc_ids
			}
		};
	}
}
