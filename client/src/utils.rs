use crate::{Client, Error};

fn healthy_path(base: String) -> String {
    format!("{}/health", base)
}

impl Client {
    pub async fn health(&self) -> Result<(), Error> {
        let client = reqwest::Client::new();
        let result = client
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
