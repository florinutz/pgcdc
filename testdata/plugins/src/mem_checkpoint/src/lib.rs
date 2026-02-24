use extism_pdk::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct CheckpointResp {
    lsn: u64,
}

#[plugin_fn]
pub fn init(_config: Vec<u8>) -> FnResult<Vec<u8>> {
    Ok(Vec::new())
}

#[plugin_fn]
pub fn load(slot_name: Vec<u8>) -> FnResult<Vec<u8>> {
    let key = String::from_utf8_lossy(&slot_name).to_string();
    match var::get::<Vec<u8>>(&key)? {
        Some(data) => Ok(data),
        None => {
            let resp = CheckpointResp { lsn: 0 };
            Ok(serde_json::to_vec(&resp)?)
        }
    }
}

#[plugin_fn]
pub fn save(input: Vec<u8>) -> FnResult<Vec<u8>> {
    #[derive(Deserialize)]
    struct SaveReq {
        slot_name: String,
        lsn: u64,
    }
    let req: SaveReq = serde_json::from_slice(&input)?;
    let resp = CheckpointResp { lsn: req.lsn };
    let data = serde_json::to_vec(&resp)?;
    var::set(&req.slot_name, &data)?;
    Ok(Vec::new())
}

#[plugin_fn]
pub fn close() -> FnResult<Vec<u8>> {
    Ok(Vec::new())
}
