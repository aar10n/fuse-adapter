//! Environment variable substitution for configuration values
//!
//! This module provides functionality to substitute environment variable
//! references in configuration strings. Variables are referenced using
//! the `${VAR_NAME}` syntax.

use once_cell::sync::Lazy;
use regex::Regex;
use std::env;

use crate::config::ConfigError;

/// Regex pattern for matching environment variable references: ${VAR_NAME}
static ENV_VAR_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").unwrap());

/// Substitute environment variable references in a string.
///
/// Variables are referenced using the `${VAR_NAME}` syntax.
/// Returns an error listing all missing variables if any are not set.
///
/// # Examples
///
/// ```ignore
/// use fuse_adapter::env::substitute_env_vars;
///
/// std::env::set_var("MY_SECRET", "secret_value");
/// let result = substitute_env_vars("token: ${MY_SECRET}").unwrap();
/// assert_eq!(result, "token: secret_value");
/// ```
pub fn substitute_env_vars(input: &str) -> Result<String, ConfigError> {
    let mut missing_vars = Vec::new();
    let mut result = input.to_string();

    // Collect all variable references
    for caps in ENV_VAR_PATTERN.captures_iter(input) {
        let full_match = caps.get(0).unwrap().as_str();
        let var_name = caps.get(1).unwrap().as_str();

        match env::var(var_name) {
            Ok(value) => {
                result = result.replace(full_match, &value);
            }
            Err(_) => {
                if !missing_vars.contains(&var_name.to_string()) {
                    missing_vars.push(var_name.to_string());
                }
            }
        }
    }

    if !missing_vars.is_empty() {
        return Err(ConfigError::ValidationError(format!(
            "Missing environment variables: {}",
            missing_vars.join(", ")
        )));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_no_substitution_needed() {
        let input = "plain text without variables";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn test_single_variable_substitution() {
        env::set_var("TEST_VAR_SINGLE", "hello");
        let result = substitute_env_vars("prefix_${TEST_VAR_SINGLE}_suffix").unwrap();
        assert_eq!(result, "prefix_hello_suffix");
        env::remove_var("TEST_VAR_SINGLE");
    }

    #[test]
    fn test_multiple_variable_substitution() {
        env::set_var("TEST_VAR_A", "alpha");
        env::set_var("TEST_VAR_B", "beta");
        let result = substitute_env_vars("${TEST_VAR_A} and ${TEST_VAR_B}").unwrap();
        assert_eq!(result, "alpha and beta");
        env::remove_var("TEST_VAR_A");
        env::remove_var("TEST_VAR_B");
    }

    #[test]
    fn test_same_variable_multiple_times() {
        env::set_var("TEST_VAR_REPEAT", "value");
        let result = substitute_env_vars("${TEST_VAR_REPEAT}-${TEST_VAR_REPEAT}").unwrap();
        assert_eq!(result, "value-value");
        env::remove_var("TEST_VAR_REPEAT");
    }

    #[test]
    fn test_missing_variable_error() {
        let result = substitute_env_vars("${NONEXISTENT_VAR_12345}");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("NONEXISTENT_VAR_12345"));
    }

    #[test]
    fn test_multiple_missing_variables_error() {
        let result = substitute_env_vars("${MISSING_A_12345} and ${MISSING_B_12345}");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("MISSING_A_12345"));
        assert!(err.to_string().contains("MISSING_B_12345"));
    }

    #[test]
    fn test_variable_with_underscores_and_numbers() {
        env::set_var("TEST_VAR_123_ABC", "complex");
        let result = substitute_env_vars("${TEST_VAR_123_ABC}").unwrap();
        assert_eq!(result, "complex");
        env::remove_var("TEST_VAR_123_ABC");
    }

    #[test]
    fn test_partial_match_not_substituted() {
        // Ensure partial patterns like $VAR or {VAR} are not matched
        let result = substitute_env_vars("$VAR and {VAR} remain unchanged").unwrap();
        assert_eq!(result, "$VAR and {VAR} remain unchanged");
    }
}
