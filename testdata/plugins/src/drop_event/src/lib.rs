use extism_pdk::*;

#[plugin_fn]
pub fn transform(_input: Vec<u8>) -> FnResult<Vec<u8>> {
    Ok(Vec::new())
}
