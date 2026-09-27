use anyhow::Result;
use chrono::{DateTime, Utc};
use stream_core::{ItemView, SearchQuery, StreamRuntime};
use stream_model::{AttentionEvent, AttentionSummary, ItemId, ItemState, SourceId};

pub struct StreamQueryService<'a> {
    runtime: &'a StreamRuntime,
}

impl<'a> StreamQueryService<'a> {
    pub fn new(runtime: &'a StreamRuntime) -> Self {
        Self { runtime }
    }

    pub async fn list(&self) -> Result<Vec<ItemView>> {
        self.runtime.list_items().await
    }

    pub async fn get(&self, item_id: &ItemId) -> Result<Option<ItemView>> {
        self.runtime.get_item(item_id).await
    }

    pub async fn unread(&self) -> Result<Vec<ItemView>> {
        self.runtime
            .search_items(SearchQuery {
                state: Some(ItemState::Unseen),
                ..Default::default()
            })
            .await
    }

    pub async fn saved(&self) -> Result<Vec<ItemView>> {
        self.runtime
            .search_items(SearchQuery {
                state: Some(ItemState::Saved),
                ..Default::default()
            })
            .await
    }

    pub async fn important(&self) -> Result<Vec<ItemView>> {
        self.runtime
            .search_items(SearchQuery {
                state: Some(ItemState::Important),
                ..Default::default()
            })
            .await
    }

    pub async fn recent(&self, after: DateTime<Utc>) -> Result<Vec<ItemView>> {
        self.runtime
            .search_items(SearchQuery {
                after: Some(after),
                ..Default::default()
            })
            .await
    }

    pub async fn search(
        &self,
        text: String,
        source_id: Option<SourceId>,
        state: Option<ItemState>,
    ) -> Result<Vec<ItemView>> {
        self.runtime
            .search_items(SearchQuery {
                text: Some(text),
                source_id,
                state,
                ..Default::default()
            })
            .await
    }

    pub async fn attention_summary(&self) -> Result<AttentionSummary> {
        self.runtime.attention_summary().await
    }

    pub async fn attention_list(&self) -> Result<Vec<AttentionEvent>> {
        self.runtime.list_attention().await
    }
}
