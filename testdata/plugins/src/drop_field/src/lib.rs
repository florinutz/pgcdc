use extism_pdk::*;
use serde_json::Value;

#[plugin_fn]
pub fn transform(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let mut event: Value = serde_json::from_slice(&input)?;
    if let Some(payload) = event.get_mut("payload") {
        if let Some(obj) = payload.as_object_mut() {
            obj.remove("secret");
        }
    }
    Ok(serde_json::to_vec(&event)?)
}
