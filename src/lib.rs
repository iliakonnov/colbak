#![feature(
    arbitrary_enum_discriminant,
    backtrace,
    exhaustive_patterns,
    generic_associated_types,
    map_first_last,
    never_type,
    once_cell,
    type_alias_impl_trait,
)]
#![cfg_attr(windows, feature(windows_by_handle))]
#![warn(clippy::pedantic, clippy::cargo)]
#![deny(
    // This project should never panic.
    // Unfortunately, much more simple `missing_panics_doc` works only on public items.
    clippy::unwrap_used, clippy::expect_used, clippy::panic
)]
#![allow(
    // Handled by lints above
    clippy::missing_panics_doc,
    // This is not a library, sorry
    clippy::missing_errors_doc,
    // I know better, what is readable. This project does not have any long literals really.
    clippy::unreadable_literal,
    // For me, it's better to make as many arms, as many variants in enum.
    clippy::match_same_arms,
    // I think that this lints reduces readability
    clippy::map_unwrap_or,
    // Again, I do not think that this lint is needed.
    clippy::module_name_repetitions,
    // This lint is useful, but too annoying
    clippy::wildcard_imports,
)]

pub use sha2::Sha256 as DefaultDigest;
pub use time::OffsetDateTime as DateTime;

#[macro_use]
pub mod logging;

#[macro_use]
pub mod utils;

pub mod cloud;
pub mod cpio;
pub mod database;
pub mod fileext;
pub mod fileinfo;
pub mod packer;
pub mod path;
pub mod serde_b64;
pub mod stream_hash;
pub mod types;
