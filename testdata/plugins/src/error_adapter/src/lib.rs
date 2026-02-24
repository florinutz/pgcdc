use extism_pdk::*;

#[plugin_fn]
pub fn init(_config: Vec<u8>) -> FnResult<Vec<u8>> {
    Ok(Vec::new())
}

#[plugin_fn]
pub fn handle(_event: Vec<u8>) -> FnResult<Vec<u8>> {
    Ok(b"simulated adapter error".to_vec())
}
