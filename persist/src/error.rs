//! WIP

use std::{error, fmt, io, sync};

#[derive(Debug)]
pub enum Error {
    IO(io::Error),
    String(String),
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IO(e)
    }
}

impl<T> From<sync::PoisonError<T>> for Error {
    fn from(e: sync::PoisonError<T>) -> Self {
        Error::String(format!("poison: {}", e))
    }
}
