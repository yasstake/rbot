// Copyright(c) 2022-2024. yasstake. All rights reserved.

use pyo3::{types::PyAnyMethods, Bound, PyAny};

/// Checks if a given Python object has a method with the specified name.
pub fn has_method(agent: &Bound<PyAny>, method_name: &str) -> bool {
    let dir  = agent.dir().unwrap();

    if dir.contains(method_name).unwrap_or(false) {
        return true;
    } else {
        return false;
    }
}
