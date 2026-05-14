use std::hash::{Hash, Hasher};

pub const EMBEDDING_DIMS: usize = 256;

/// In-process semantic memory backed by deterministic hashed embeddings.
///
/// This keeps Phase 3 semantic retrieval available without requiring a hosted
/// embedding service. Tokens and adjacent token pairs are projected into a
/// fixed-size signed vector, normalized, and compared with cosine similarity.
#[derive(Default)]
pub struct MemoryIndex {
    entries: Vec<MemoryEntry>,
}

struct MemoryEntry {
    id: String,
    content: String,
    embedding: Vec<f32>,
}

impl MemoryIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, id: impl Into<String>, content: &str) {
        let embedding = embed_text(content);
        if embedding.iter().any(|v| *v != 0.0) {
            self.entries.push(MemoryEntry {
                id: id.into(),
                content: content.to_string(),
                embedding,
            });
        }
    }

    /// Returns content snippets (up to 300 chars each) for the top_k most relevant entries.
    pub fn search(&self, query: &str, top_k: usize) -> Vec<String> {
        if self.entries.is_empty() || query.trim().is_empty() {
            return Vec::new();
        }
        let query_embedding = embed_text(query);
        if query_embedding.iter().all(|v| *v == 0.0) {
            return Vec::new();
        }

        let mut scored: Vec<(&MemoryEntry, f32)> = self
            .entries
            .iter()
            .map(|entry| (entry, cosine(&query_embedding, &entry.embedding)))
            .filter(|(_, score)| *score > 0.05)
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(top_k);

        scored
            .into_iter()
            .map(|(entry, _)| {
                let snippet: String = entry.content.chars().take(300).collect();
                format!("[{}]: {}", entry.id, snippet)
            })
            .collect()
    }
}

pub trait EmbeddingProvider: Send + Sync {
    fn provider_name(&self) -> &str;
    fn model_name(&self) -> &str;
    fn embed(&self, text: &str) -> Vec<f32>;
}

#[derive(Debug, Clone)]
pub struct HashedEmbeddingProvider;

impl EmbeddingProvider for HashedEmbeddingProvider {
    fn provider_name(&self) -> &str {
        "offline"
    }

    fn model_name(&self) -> &str {
        "hashed-embedding-v1"
    }

    fn embed(&self, text: &str) -> Vec<f32> {
        embed_text(text)
    }
}

#[derive(Debug, Clone)]
pub struct ProviderEmbeddingProvider {
    pub provider_name: String,
    pub model_name: String,
}

impl EmbeddingProvider for ProviderEmbeddingProvider {
    fn provider_name(&self) -> &str {
        &self.provider_name
    }

    fn model_name(&self) -> &str {
        &self.model_name
    }

    fn embed(&self, text: &str) -> Vec<f32> {
        // Provider-backed embeddings can be swapped in by callers with network
        // access. The deterministic projection remains the offline fallback.
        embed_text(text)
    }
}

pub fn embed_text(text: &str) -> Vec<f32> {
    let tokens = tokens(text);
    if tokens.is_empty() {
        return vec![0.0; EMBEDDING_DIMS];
    }

    let mut vector = vec![0.0; EMBEDDING_DIMS];
    for token in &tokens {
        project_feature(token, 1.0, &mut vector);
    }
    for pair in tokens.windows(2) {
        project_feature(&format!("{}:{}", pair[0], pair[1]), 1.25, &mut vector);
    }
    normalize(&mut vector);
    vector
}

fn tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .map(str::to_lowercase)
        .filter(|word| word.len() >= 3)
        .collect()
}

fn project_feature(feature: &str, weight: f32, vector: &mut [f32]) {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    feature.hash(&mut hasher);
    let hash = hasher.finish();
    let idx = hash as usize % vector.len();
    let sign = if (hash >> 63) == 0 { 1.0 } else { -1.0 };
    vector[idx] += sign * weight;
}

fn normalize(vector: &mut [f32]) {
    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in vector {
            *value /= norm;
        }
    }
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(left, right)| left * right).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_relevant_entries() {
        let mut idx = MemoryIndex::new();
        idx.add("auth", "authentication login logout session token user");
        idx.add("database", "postgres sql query table migration schema");
        idx.add(
            "api",
            "authentication endpoint request response bearer token",
        );

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
