//! Stream reasoning: synthesis across sources and evidence-grounded answers
//! over knowledge retrieved from Stream. Nothing here touches storage.

pub mod reasoning;
pub mod synthesis;

pub use reasoning::{
    bundle_prompt, content_terms, verify_answer, AnswerProposal, Bundle, BundleClaim, BundleConnection, BundleContext,
    BundleEvidence, BundleInsight, BundleSignal, Intent, LocalReasoner, ModelReasoner, Reasoner, RelatedPair, Scope,
    Statement, Sufficiency, TimelineEntry, INSUFFICIENT,
};
pub use synthesis::{
    verify_synthesis, LocalSynthesizer, ModelSynthesizer, Observation, ObservationEvidence, SynthesisInput, SynthesisProposal,
    Synthesizer,
};
