pub fn get_parent(path: &str) -> String {
    match path.rfind('/') {
        Some(idx) => path[..idx].to_string(),
        None => "".to_string(),
    }
}
