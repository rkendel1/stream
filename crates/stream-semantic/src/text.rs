use std::collections::HashMap;

pub(crate) const STOPWORDS: &[&str] = &[
    "a", "about", "above", "after", "again", "against", "all", "also", "am", "an", "and", "any", "are", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can", "could", "did", "do",
    "does", "doing", "down", "during", "each", "even", "every", "few", "for", "from", "further", "get", "gets", "got",
    "had", "has", "have", "having", "he", "her", "here", "hers", "him", "his", "how", "however", "i", "if", "in",
    "into", "is", "it", "its", "itself", "just", "last", "like", "made", "make", "makes", "many", "may", "me", "might",
    "more", "most", "much", "must", "my", "new", "news", "no", "nor", "not", "now", "of", "off", "on", "once", "one",
    "only", "or", "other", "our", "ours", "out", "over", "own", "per", "post", "read", "same", "says", "see", "she",
    "should", "since", "so", "some", "such", "than", "that", "the", "their", "them", "then", "there", "these", "they",
    "this", "those", "through", "to", "today", "too", "two", "under", "until", "up", "us", "use", "used", "using",
    "very", "via", "want", "was", "way", "we", "week", "well", "were", "what", "when", "where", "which", "while", "who",
    "whom", "why", "will", "with", "within", "without", "would", "year", "yet", "you", "your", "yours", "blog",
    "article", "update", "updates", "announcing", "introducing", "announces", "announced", "available", "release",
    "released", "releases", "version", "latest", "first", "look", "things", "thing", "lets", "let", "don", "doesn",
    "isn", "it's", "that's", "https", "http", "www", "com", "org", "html",
];

pub(crate) fn is_stopword(token: &str) -> bool {
    STOPWORDS.contains(&token)
}

/// A deliberately small, deterministic English stemmer: enough to make
/// "containers" meet "container" and "runtimes" meet "runtime".
pub fn stem(token: &str) -> String {
    let token = token.to_lowercase();
    let len = token.chars().count();
    if len <= 3 {
        return token;
    }
    for (suffix, min_stem) in [("ies", 3usize), ("ing", 4), ("ed", 4), ("es", 4), ("s", 3)] {
        if let Some(stem) = token.strip_suffix(suffix) {
            if stem.chars().count() >= min_stem && !stem.ends_with('s') {
                return match suffix {
                    "ies" => format!("{stem}y"),
                    "es" if !(stem.ends_with("ch") || stem.ends_with("sh") || stem.ends_with('x')) => format!("{stem}e"),
                    _ => stem.to_owned(),
                };
            }
        }
    }
    token
}

/// Lowercased word tokens, keeping version-like tokens ("1.99") intact.
pub(crate) fn tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !(c.is_alphanumeric() || c == '.' || c == '-' || c == '+' || c == '#' || c == '\''))
        .map(|token| token.trim_matches(|c: char| c == '.' || c == '-' || c == '\'').to_lowercase())
        .filter(|token| !token.is_empty())
        .collect()
}

/// Distinctive, stemmed terms of a text: the vocabulary Stream uses to relate
/// items to each other and to the user's context.
pub fn key_terms(title: &str, content: &str, limit: usize) -> Vec<String> {
    let mut weights: HashMap<String, f32> = HashMap::new();
    let mut first_seen: HashMap<String, usize> = HashMap::new();
    let mut position = 0usize;
    let content = content.get(..content.len().min(4000)).unwrap_or(content);
    for (text, weight) in [(title, 3.0f32), (content, 1.0)] {
        for token in tokens(text) {
            position += 1;
            if is_stopword(&token) || token.chars().all(|c| c.is_ascii_digit() || c == '.') {
                continue;
            }
            if token.chars().count() < 3 && !matches!(token.as_str(), "ai" | "vm" | "ml" | "ui" | "go" | "os" | "db") {
                continue;
            }
            let term = stem(&token);
            *weights.entry(term.clone()).or_default() += weight;
            first_seen.entry(term).or_insert(position);
        }
    }
    let mut terms = weights.into_iter().collect::<Vec<_>>();
    terms.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| first_seen[&a.0].cmp(&first_seen[&b.0]))
    });
    terms.into_iter().take(limit).map(|(term, _)| term).collect()
}

/// Split text into verbatim sentences (every returned sentence is a
/// substring of the input), so they can serve as evidence excerpts.
pub fn sentences(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let bytes = text.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        let boundary = byte == b'\n'
            || (matches!(byte, b'.' | b'!' | b'?')
                && bytes.get(index + 1).map(|next| next.is_ascii_whitespace()).unwrap_or(true)
                // "v1.2 " or "e.g. " are not sentence ends when followed by lowercase.
                && !bytes
                    .get(index + 2)
                    .map(|next| next.is_ascii_lowercase())
                    .unwrap_or(false));
        if boundary {
            let end = if byte == b'\n' { index } else { index + 1 };
            if let Some(sentence) = text.get(start..end) {
                let sentence = sentence.trim();
                if sentence.chars().filter(|c| c.is_alphanumeric()).count() >= 3 {
                    out.push(sentence);
                }
            }
            start = index + 1;
        }
        index += 1;
    }
    if let Some(rest) = text.get(start..) {
        let rest = rest.trim();
        if rest.chars().filter(|c| c.is_alphanumeric()).count() >= 3 {
            out.push(rest);
        }
    }
    out
}

/// Does `haystack` contain `phrase` as whole words (case-insensitive, stemmed)?
pub(crate) fn contains_phrase(haystack_stems: &[String], phrase: &str) -> bool {
    let needle = tokens(phrase).iter().map(|token| stem(token)).collect::<Vec<_>>();
    if needle.is_empty() || needle.len() > haystack_stems.len() {
        return false;
    }
    haystack_stems.windows(needle.len()).any(|window| window == needle.as_slice())
}

pub(crate) fn stems(text: &str) -> Vec<String> {
    tokens(text).iter().map(|token| stem(token)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stemming_unifies_simple_inflections() {
        assert_eq!(stem("containers"), "container");
        assert_eq!(stem("runtimes"), "runtime");
        assert_eq!(stem("libraries"), "library");
        assert_eq!(stem("adds"), "add");
        assert_eq!(stem("patches"), "patch");
        assert_eq!(stem("rust"), "rust");
        assert_eq!(stem("process"), "process");
    }

    #[test]
    fn sentences_are_verbatim_substrings() {
        let text = "Rust 1.99 ships today. It adds async closures!\nMore at the blog";
        let parts = sentences(text);
        assert_eq!(parts, vec!["Rust 1.99 ships today.", "It adds async closures!", "More at the blog"]);
        assert!(parts.iter().all(|part| text.contains(part)));
    }

    #[test]
    fn key_terms_prefer_title_vocabulary() {
        let terms = key_terms("Apple Container adds Linux VMs", "The container runtime is portable.", 5);
        assert_eq!(terms[0], "container", "title + body outranks title alone");
        assert_eq!(terms[1], "apple");
    }
}
