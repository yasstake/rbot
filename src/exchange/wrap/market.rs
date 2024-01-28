use pyo3::*;


trait WrapMarketTrait {
    fn _start_user_stream(&self) -> PyResult<String>;
}



#[pyclass]
struct WrapMarket {
}

#[pymethods]
impl WrapMarket {
    fn start_user_stream(&self) -> PyResult<String> {
        self._start_user_stream()
    }
}

impl WrapMarketTrait for WrapMarket {
    fn _start_user_stream(&self) -> PyResult<String> {
        Ok("".to_string())
    }
}
