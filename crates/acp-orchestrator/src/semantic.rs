use std::collections::BTreeMap;

/// Lightweight in-process keyword index using TF cosine similarity.
/// No external ML dependencies — pure term frequency vectors.
#[derive(Default)]
pub struct MemoryIndex {
    entries: Vec<MemoryEntry>,
}

struct MemoryEntry {
    id: String,
    content: String,
    tf: BTreeMap<String, f64>,
}

impl MemoryIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, id: impl Into<String>, content: &str) {
        let tf = compute_tf(content);
        self.entries.push(MemoryEntry {
            id: id.into(),
            content: content.to_string(),
            tf,
        });
    }

    /// Returns content snippets (up to 300 chars each) for the top_k most relevant entries.
    pub fn search(&self, query: &str, top_k: usize) -> Vec<String> {
        if self.entries.is_empty() || query.trim().is_empty() {
            return Vec::new();
        }
        let qtf = compute_tf(query);
        if qtf.is_empty() {
            return Vec::new();
        }

        let mut scored: Vec<(&MemoryEntry, f64)> = self
            .entries
            .iter()
            .map(|e| (e, cosine_sim(&qtf, &e.tf)))
            .filter(|(_, s)| *s > 0.01)
            .collect();

        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(top_k);

        scored
            .into_iter()
            .map(|(e, _)| {
                let snippet: String = e.content.chars().take(300).collect();
                format!("[{}]: {}", e.id, snippet)
            })
            .collect()
    }
}

fn compute_tf(text: &str) -> BTreeMap<String, f64> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut total = 0usize;
    for word in text.split(|c: char| !c.is_alphanumeric()) {
        let word = word.to_lowercase();
        if word.len() >= 3 {
            *counts.entry(word).or_default() += 1;
            total += 1;
        }
    }
    if total == 0 {
        return BTreeMap::new();
    }
    counts
        .into_iter()
        .map(|(k, v)| (k, v as f64 / total as f64))
        .collect()
}

fn cosine_sim(a: &BTreeMap<String, f64>, b: &BTreeMap<String, f64>) -> f64 {
    let dot: f64 = a.iter().filter_map(|(k, va)| b.get(k).map(|vb| va * vb)).sum();
    if dot == 0.0 {
        return 0.0;
    }
    let na: f64 = a.values().map(|v| v * v).sum::<f64>().sqrt();
    let nb: f64 = b.values().map(|v| v * v).sum::<f64>().sqrt();
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na * nb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_relevant_entries() {
        let mut idx = MemoryIndex::new();
        idx.add("auth", "authentication login logout session token user");
        idx.add("database", "postgres sql query table migration schema");
        idx.add("api", "authentication endpoint request response bearer token");

        let results = idx.search("user authentication token", 2);
        assert!(!results.is_empty());
        assert!(results[0].contains("auth") || results[0].contains("api"));
    }

    #[test]
    fn empty_index_returns_empty() {
        let idx = MemoryIndex::new();
        assert!(idx.search("anything", 3).is_empty());
    }

    #[test]
    fn unrelated_query_returns_empty() {
        let mut idx = MemoryIndex::new();
        idx.add("step1", "rust tokio async await future");
        let results = idx.search("xyz qrst uvwx", 3);
        assert!(results.is_empty());
    }
}
