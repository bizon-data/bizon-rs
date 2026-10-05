//! Generated BigQuery Storage Write API v1 client and server, from the protos vendored under `/proto`.

#[allow(clippy::all, clippy::pedantic)]
pub mod google {
    pub mod api {
        tonic::include_proto!("google.api");
    }
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
    pub mod cloud {
        pub mod bigquery {
            pub mod storage {
                pub mod v1 {
                    tonic::include_proto!("google.cloud.bigquery.storage.v1");
                }
            }
        }
    }
}

pub use google::cloud::bigquery::storage::v1 as storage;
pub use google::rpc::Status as RpcStatus;
