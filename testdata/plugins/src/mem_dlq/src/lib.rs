use extism_pdk::*;

#[plugin_fn]
pub fn init(_config: Vec<u8>) -> FnResult<Vec<u8>> {
    Ok(Vec::new())
}

#[plugin_fn]
pub fn record(_input: Vec<u8>) -> FnResult<Vec<u8>> {
    Ok(Vec::new())
}

#[plugin_fn]
pub fn close() -> FnResult<Vec<u8>> {
    Ok(Vec::new())
}
