use pyo3::{pyfunction, PyResult, exceptions::PyValueError};



#[pyfunction]
pub fn rest_get(
    url: &str,
) -> PyResult<String> {
    log::debug!("REST url={}", url);

    let response = reqwest::blocking::get(url);

    match response {
        Ok(response) => {
            match response.text() {
                Ok(text) => {
                    return Ok(text);
                },
                Err(e) => {
                    return Err(PyValueError::new_err(e.to_string()));
                }
            }
        }
        Err(e) => {
            log::warn!("REST ERROR {:?}", e);
            return Err(PyValueError::new_err(e.to_string()));
        }
    };
}
