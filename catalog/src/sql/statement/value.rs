use super::SecretName;
use lyra_meta::proto::pb_catalog::Scram;
use std::fmt;

#[derive(Clone, PartialEq, Eq)]
pub enum Value {
    Null,
    Literal(String),
    Secret(SecretName),
    Scram(Scram),
}

impl fmt::Debug for Value {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => formatter.write_str("Null"),
            Self::Literal(_) => formatter.write_str("Literal([REDACTED])"),
            Self::Secret(secret) => formatter.debug_tuple("Secret").field(secret).finish(),
            Self::Scram(_) => formatter.write_str("Scram([REDACTED])"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_redacts_literal_and_scram_values() {
        assert_eq!(format!("{:?}", Value::Null), "Null");
        assert_eq!(
            format!("{:?}", Value::Literal("secret".to_string())),
            "Literal([REDACTED])"
        );
        assert_eq!(
            format!(
                "{:?}",
                Value::Secret(SecretName::new(None, "name".to_string()))
            ),
            "Secret(SecretName { schema: None, name: \"name\" })"
        );
        let scram = lyra_meta::utils::scram::make_scram("password");
        assert_eq!(format!("{:?}", Value::Scram(scram)), "Scram([REDACTED])");
    }
}
