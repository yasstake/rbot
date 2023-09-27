use pyo3::{PyAny, PyObject, Python};

/// Checks if a given Python object has a method with the specified name.
pub fn has_method(agent: &PyAny, method_name: &str) -> bool {
    if agent.dir().contains(method_name).unwrap_or(false) {
        return true;
    } else {
        return false;
    }
}
