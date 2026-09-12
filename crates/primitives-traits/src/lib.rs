//! Shared traits and error types for sidecar coordination.
//!
//! This crate defines the integration boundaries between the coordinator
//! and its external dependencies (publisher, mailbox, builder control). By
//! sitting below the coordinator in the dependency graph, these traits
//! can be implemented by lower-level crates without circular dependencies.

pub mod builder_control;
pub mod error;
pub mod l2_bridge;
pub mod mailbox;
pub mod publisher;
pub mod put_inbox;

pub use builder_control::XtBuilderClient;
pub use error::CoordinatorError;
pub use l2_bridge::{L2BridgeTxBuilder, SendAbortEthParams, SendAbortTokenParams};
pub use mailbox::MailboxSender;
pub use publisher::PublisherClient;
pub use put_inbox::PutInboxBuilder;
