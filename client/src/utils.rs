use crate::{Client, Error};
use commonware_parallel::Strategy;

fn healthy_path(base: String) -> String {
    format!("{base}/health")
}

impl<S: Strategy> Client<S> {
    pub async fn health(&self) -> Result<(), Error> {
        let result = self
            .http_client
            .get(healthy_path(self.uri.clone()))
            .send()
            .await
            .map_err(Error::from)?;
        if !result.status().is_success() {
            return Err(Error::Failed(result.status()));
        }
        Ok(())
    }
}
