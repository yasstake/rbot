use pyo3::{PyObject, PyAny, Python};




/// Checks if a given Python object has a method with the specified name.
pub fn has_method(agent: &PyObject, method_name: &str) -> bool {
    Python::with_gil(|py| {
        let agent = agent.as_ref(py);

        if agent.dir().contains(method_name).unwrap_or(false) {
            return true;
        }
        else {
            return false;
        }
    })
}

