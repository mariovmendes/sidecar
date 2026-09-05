//! Single-line, greppable XT lifecycle tracing.
//!
//! Every stage of the cross-chain transaction pipeline emits exactly one log
//! line of the form:
//!
//! ```text
//! XTFLOW stage=<stage> instance_id=<id> key=value ...
//! ```
//!
//! The whole record lives on one line inside the log *message* (not as
//! tracing fields), so it survives every subscriber format (`pretty`, `json`,
//! plain) unchanged and can be grepped per instance:
//!
//! ```sh
//! docker logs sidecar-a 2>&1 | grep XTFLOW | grep <instance_id>
//! ```

/// Emit one `XTFLOW` line. First argument is the stage name (a string
/// literal), followed by `key = value` pairs whose values implement
/// `Display`.
///
/// ```ignore
/// xtflow!("vote", instance_id = id, vote = true, chain = self.chain_id);
/// ```
#[macro_export]
macro_rules! xtflow {
    ($($args:tt)*) => {
        ::tracing::info!(target: "xtflow", "{}", $crate::xtflow_line!($($args)*))
    };
}

/// Builds the `XTFLOW` line without logging it. Exists so the format is
/// testable without a subscriber; use [`xtflow!`] instead.
#[macro_export]
macro_rules! xtflow_line {
    ($stage:literal $(, $key:ident = $value:expr)* $(,)?) => {
        format!(
            concat!("XTFLOW stage=", $stage $(, " ", stringify!($key), "={}")*)
            $(, $value)*
        )
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn renders_one_greppable_line() {
        let line = crate::xtflow_line!(
            "builder_submit",
            instance_id = "ab12",
            chain = 77777,
            tx_count = 2,
        );
        assert_eq!(
            line,
            "XTFLOW stage=builder_submit instance_id=ab12 chain=77777 tx_count=2"
        );
        assert!(!line.contains('\n'), "must stay on a single line");
        assert_eq!(crate::xtflow_line!("state_dump"), "XTFLOW stage=state_dump");
    }
}
