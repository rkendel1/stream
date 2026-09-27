use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityDefinition {
    pub name: String,
    pub version: u32,
    pub description: String,
    pub operations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppPortManifest {
    pub application: ApplicationDescriptor,
    pub capabilities: Vec<CapabilityDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationDescriptor {
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: String,
}

impl AppPortManifest {
    pub fn stream() -> Self {
        Self {
            application: ApplicationDescriptor {
                id: "com.rkendel.stream".into(),
                name: "Stream".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                description: "A local-first information runtime built on FeltDB and AppPort.".into(),
            },
            capabilities: vec![
                CapabilityDefinition {
                    name: "appport.manifest".into(),
                    version: 1,
                    description: "Reserved AppPort manifest capability.".into(),
                    operations: vec!["get".into()],
                },
                CapabilityDefinition {
                    name: "appport.ping".into(),
                    version: 1,
                    description: "Reserved AppPort ping capability.".into(),
                    operations: vec!["ping".into()],
                },
                CapabilityDefinition {
                    name: "stream.source".into(),
                    version: 1,
                    description: "Manage Stream sources.".into(),
                    operations: vec!["create", "list", "get", "refresh", "pause", "resume"].into_iter().map(String::from).collect(),
                },
                CapabilityDefinition {
                    name: "stream.item".into(),
                    version: 1,
                    description: "Read and mutate Stream item state.".into(),
                    operations: vec!["get", "state", "mark_read", "save", "dismiss", "mark_important", "archive"].into_iter().map(String::from).collect(),
                },
                CapabilityDefinition {
                    name: "stream.query".into(),
                    version: 1,
                    description: "Query normalized Stream items.".into(),
                    operations: vec!["list", "unread", "saved", "important", "recent"].into_iter().map(String::from).collect(),
                },
                CapabilityDefinition {
                    name: "stream.search".into(),
                    version: 1,
                    description: "Search the durable Stream corpus.".into(),
                    operations: vec!["search"].into_iter().map(String::from).collect(),
                },
                CapabilityDefinition {
                    name: "stream.attention".into(),
                    version: 1,
                    description: "Summarize and resolve attention state.".into(),
                    operations: vec!["summary", "list", "resolve"].into_iter().map(String::from).collect(),
                },
            ],
        }
    }

    pub fn has_capability(&self, name: &str) -> bool {
        self.capabilities.iter().any(|capability| capability.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::AppPortManifest;

    #[test]
    fn manifest_contains_stream_capabilities() {
        let manifest = AppPortManifest::stream();
        assert!(manifest.has_capability("stream.source"));
        assert!(manifest.has_capability("stream.item"));
        assert!(manifest.has_capability("stream.query"));
        assert!(manifest.has_capability("stream.search"));
        assert!(manifest.has_capability("stream.attention"));
    }
}
