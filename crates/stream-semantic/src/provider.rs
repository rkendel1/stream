//! Model providers: the only place Stream knows how to talk to a model.
//!
//! The rest of Stream sees [`ModelProvider`] — structured JSON in, structured
//! JSON out — and never learns which provider or model is behind it.
//! Configuration lives here, at the provider/runtime layer.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// A request for one structured completion. The provider must return a JSON
/// document conforming to `schema`; Stream never parses free-form prose.
#[derive(Debug, Clone, Serialize)]
pub struct ModelRequest {
    pub system: String,
    pub user: String,
    pub schema_name: String,
    pub schema: Value,
    pub max_tokens: u32,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ModelError {
    #[error("model provider unavailable: {0}")]
    Unavailable(String),
    #[error("model provider returned HTTP {status}: {body}")]
    Http { status: u16, body: String },
    #[error("model output was not valid structured output: {0}")]
    Malformed(String),
    #[error("model refused: {0}")]
    Refused(String),
}

#[async_trait]
pub trait ModelProvider: Send + Sync {
    /// Stable identifier for audit records (e.g. `openai-compatible:llama3.2`).
    fn id(&self) -> String;

    /// Return the raw JSON text of a structured completion.
    async fn complete_json(&self, request: &ModelRequest) -> Result<String, ModelError>;
}

/// Speaks the OpenAI-compatible chat completions API, which local runtimes
/// (Ollama, llama.cpp server, LM Studio, vLLM) and many remote services share.
pub struct OpenAiCompatibleProvider {
    client: reqwest::Client,
    endpoint: Url,
    model: String,
    api_key: Option<String>,
}

impl OpenAiCompatibleProvider {
    pub fn new(base_url: Url, model: impl Into<String>, api_key: Option<String>, timeout: Duration) -> Self {
        let mut base = base_url.to_string();
        if !base.ends_with('/') {
            base.push('/');
        }
        let endpoint = Url::parse(&base).and_then(|base| base.join("chat/completions")).unwrap_or(base_url);
        Self {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .connect_timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client should build"),
            endpoint,
            model: model.into(),
            api_key,
        }
    }

    fn body(&self, request: &ModelRequest, schema_mode: bool) -> Value {
        let response_format = if schema_mode {
            json!({
                "type": "json_schema",
                "json_schema": { "name": request.schema_name, "strict": true, "schema": request.schema }
            })
        } else {
            json!({ "type": "json_object" })
        };
        json!({
            "model": self.model,
            "temperature": 0,
            "max_tokens": request.max_tokens,
            "response_format": response_format,
            "messages": [
                { "role": "system", "content": format!(
                    "{}\n\nRespond with a single JSON object that conforms to this JSON Schema:\n{}",
                    request.system, request.schema
                ) },
                { "role": "user", "content": request.user }
            ]
        })
    }

    async fn post(&self, body: &Value) -> Result<Value, ModelError> {
        let mut builder = self.client.post(self.endpoint.clone()).json(body);
        if let Some(key) = &self.api_key {
            builder = builder.bearer_auth(key);
        }
        let response = builder.send().await.map_err(|error| ModelError::Unavailable(error.to_string()))?;
        let status = response.status();
        let text = response.text().await.map_err(|error| ModelError::Unavailable(error.to_string()))?;
        if !status.is_success() {
            return Err(ModelError::Http { status: status.as_u16(), body: text.chars().take(500).collect() });
        }
        serde_json::from_str(&text).map_err(|error| ModelError::Malformed(format!("response is not JSON: {error}")))
    }
}

#[async_trait]
impl ModelProvider for OpenAiCompatibleProvider {
    fn id(&self) -> String {
        format!("openai-compatible:{}", self.model)
    }

    async fn complete_json(&self, request: &ModelRequest) -> Result<String, ModelError> {
        let response = match self.post(&self.body(request, true)).await {
            // Some local servers do not support JSON-schema output yet; the
            // schema is also in the instructions, so plain JSON mode works.
            Err(ModelError::Http { status: 400 | 422, .. }) => self.post(&self.body(request, false)).await?,
            other => other?,
        };
        let choice = response
            .get("choices")
            .and_then(|choices| choices.get(0))
            .ok_or_else(|| ModelError::Malformed("no choices in response".into()))?;
        let message = choice.get("message").ok_or_else(|| ModelError::Malformed("no message in choice".into()))?;
        if let Some(refusal) = message.get("refusal").and_then(Value::as_str) {
            return Err(ModelError::Refused(refusal.to_owned()));
        }
        if choice.get("finish_reason").and_then(Value::as_str) == Some("length") {
            return Err(ModelError::Malformed("output was truncated".into()));
        }
        message
            .get("content")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| ModelError::Malformed("message has no text content".into()))
    }
}

/// Provider configuration. The core product boots without any of it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderConfig {
    /// `none`, `ollama`, or `openai-compatible`.
    pub kind: String,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub timeout_secs: u64,
}

impl ProviderConfig {
    /// Read `STREAM_MODEL_PROVIDER`, `STREAM_MODEL`, `STREAM_MODEL_BASE_URL`,
    /// `STREAM_MODEL_API_KEY`, and `STREAM_MODEL_TIMEOUT_SECS`.
    pub fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.trim().is_empty());
        Self {
            kind: var("STREAM_MODEL_PROVIDER").unwrap_or_else(|| "none".into()).to_ascii_lowercase(),
            base_url: var("STREAM_MODEL_BASE_URL"),
            model: var("STREAM_MODEL"),
            api_key: var("STREAM_MODEL_API_KEY"),
            timeout_secs: var("STREAM_MODEL_TIMEOUT_SECS").and_then(|v| v.parse().ok()).unwrap_or(120),
        }
    }

    /// Build the configured provider, or `None` for local-only intelligence.
    pub fn build(&self) -> Result<Option<Arc<dyn ModelProvider>>, String> {
        let default_base = match self.kind.as_str() {
            "none" | "" | "local" => return Ok(None),
            "ollama" => "http://127.0.0.1:11434/v1",
            "openai-compatible" | "openai_compatible" => "http://127.0.0.1:8080/v1",
            other => return Err(format!("unknown STREAM_MODEL_PROVIDER '{other}' (expected none, ollama, or openai-compatible)")),
        };
        let model = self.model.clone().ok_or("STREAM_MODEL must name the model to use")?;
        let base = self.base_url.clone().unwrap_or_else(|| default_base.into());
        let base = Url::parse(&base).map_err(|error| format!("invalid STREAM_MODEL_BASE_URL: {error}"))?;
        Ok(Some(Arc::new(OpenAiCompatibleProvider::new(
            base,
            model,
            self.api_key.clone(),
            Duration::from_secs(self.timeout_secs),
        ))))
    }
}

/// Strip a Markdown code fence some local models wrap JSON in.
pub fn strip_code_fence(text: &str) -> &str {
    let trimmed = text.trim();
    let Some(rest) = trimmed.strip_prefix("```") else { return trimmed };
    let rest = rest.trim_start_matches(|c: char| c.is_ascii_alphabetic());
    rest.strip_suffix("```").unwrap_or(rest).trim()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_provider_is_the_default() {
        let config = ProviderConfig { kind: "none".into(), base_url: None, model: None, api_key: None, timeout_secs: 5 };
        assert!(config.build().unwrap().is_none());
    }

    #[test]
    fn ollama_needs_only_a_model_name() {
        let config = ProviderConfig { kind: "ollama".into(), base_url: None, model: Some("llama3.2".into()), api_key: None, timeout_secs: 5 };
        let provider = config.build().unwrap().unwrap();
        assert_eq!(provider.id(), "openai-compatible:llama3.2");
        let missing = ProviderConfig { model: None, ..config };
        assert!(missing.build().is_err());
    }

    #[test]
    fn strips_fences() {
        assert_eq!(strip_code_fence("```json\n{\"a\":1}\n```"), "{\"a\":1}");
        assert_eq!(strip_code_fence(" {\"a\":1} "), "{\"a\":1}");
    }
}
