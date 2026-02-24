use extism_pdk::*;

#[plugin_fn]
pub fn transform(input: Vec<u8>) -> FnResult<Vec<u8>> {
    Ok(input)
}
