use serde_json::{Map, Value};

/// Transforms JSON before differential comparison.
pub type Normalizer = fn(&Value) -> Value;

/// Return an order-stable JSON value while preserving array order.
///
/// serde_json's default map representation already sorts object keys. This
/// function also recursively inserts keys in sorted order so canonicalization
/// remains order-independent if the workspace enables `preserve_order` later.
#[must_use]
pub fn canonical(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
        Value::Object(object) => {
            let mut entries: Vec<_> = object.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));

            let mut sorted = Map::new();
            for (key, value) in entries {
                sorted.insert(key.clone(), canonical(value));
            }
            Value::Object(sorted)
        }
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::canonical;
    use serde_json::json;

    #[test]
    fn canonical_sorts_object_keys_recursively() {
        let value = json!({"b": {"z": 1, "a": 2}, "a": [ {"y": 1, "x": 2} ]});
        let got = serde_json::to_string(&canonical(&value)).unwrap();
        assert_eq!(got, r#"{"a":[{"x":2,"y":1}],"b":{"a":2,"z":1}}"#);
    }

    #[test]
    fn canonical_preserves_array_order_and_scalars() {
        let value = json!([3, 1, 2, {"k": null}, "s", true]);
        let got = serde_json::to_string(&canonical(&value)).unwrap();
        assert_eq!(got, r#"[3,1,2,{"k":null},"s",true]"#);
    }
}
