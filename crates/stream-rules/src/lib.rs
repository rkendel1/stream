use stream_model::{Item, Rule, RuleAction};

pub fn rule_matches(rule: &Rule, item: &Item) -> bool {
    if !rule.enabled {
        return false;
    }

    if let Some(source_id) = &rule.source_filter {
        if &item.source_id != source_id {
            return false;
        }
    }

    if let Some(source_kind) = rule.source_kind_filter {
        if item.source_kind != source_kind {
            return false;
        }
    }

    if let Some(pattern) = &rule.title_pattern {
        if !contains_case_insensitive(&item.title, pattern) {
            return false;
        }
    }

    if let Some(pattern) = &rule.content_pattern {
        if !contains_case_insensitive(&item.content_text, pattern) {
            return false;
        }
    }

    if let Some(pattern) = &rule.url_pattern {
        match &item.canonical_url {
            Some(url) if contains_case_insensitive(url.as_str(), pattern) => {}
            _ => return false,
        }
    }

    if let Some(pattern) = &rule.author_pattern {
        match &item.author {
            Some(author) if contains_case_insensitive(author, pattern) => {}
            _ => return false,
        }
    }

    if let Some(after) = rule.published_after {
        match item.published_at {
            Some(published_at) if published_at >= after => {}
            _ => return false,
        }
    }

    true
}

pub fn action_label(action: RuleAction) -> &'static str {
    match action {
        RuleAction::Retain => "retain",
        RuleAction::Save => "save",
        RuleAction::MarkImportant => "mark_important",
        RuleAction::CreateAttentionEvent => "create_attention_event",
    }
}

fn contains_case_insensitive(value: &str, pattern: &str) -> bool {
    value.to_ascii_lowercase().contains(&pattern.to_ascii_lowercase())
}
