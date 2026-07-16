use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Hash20(pub String);

async_graphql::scalar!(Hash20, "Hash20");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Hash32(pub String);

async_graphql::scalar!(Hash32, "Hash32");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Date(pub String);

async_graphql::scalar!(Date, "Date");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DateTime(pub String);

async_graphql::scalar!(DateTime, "DateTime");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Duration(pub String);

async_graphql::scalar!(Duration, "Duration");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Year(pub String);

async_graphql::scalar!(Year, "Year");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Void(pub ());

async_graphql::scalar!(Void, "Void");
