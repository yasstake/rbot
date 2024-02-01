// Copyright(c) 2022-2023. yasstake. All rights reserved.

use pyo3::PyAny;

/// Checks if a given Python object has a method with the specified name.
pub fn has_method(agent: &PyAny, method_name: &str) -> bool {
    if agent.dir().contains(method_name).unwrap_or(false) {
        return true;
    } else {
        return false;
    }
}
