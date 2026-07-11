use std::fmt;

/// A value whose formatting is always redacted.
///
/// Access is explicit so secrets cannot leak through derived formatting.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret<T>(T);

impl<T> Secret<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &T {
        &self.0
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret([REDACTED])")
    }
}

impl<T> fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[cfg(test)]
mod tests {
    use super::Secret;

    #[test]
    fn formatting_never_exposes_value() {
        let value = Secret::new("canary-secret");
        assert_eq!(value.to_string(), "[REDACTED]");
        assert_eq!(format!("{value:?}"), "Secret([REDACTED])");
    }
}
