//! gRPC handlers: translate requests into `node::Request`s and wait for the driver's reply.

use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status, Streaming};

use crate::node::{self, Error};
use crate::pb;
use raft::{ConfChange, Member};

pub const LEADER_HEADER: &str = "x-leader";

impl From<Error> for Status {
    fn from(e: Error) -> Self {
        match e {
            Error::NotLeader(addr) => {
                let mut s = Status::failed_precondition("not leader");
                if let Some(a) = addr.and_then(|a| a.parse().ok()) {
                    s.metadata_mut().insert(LEADER_HEADER, a);
                }
                s
            }
            Error::Unavailable(m) => Status::unavailable(m),
            Error::Invalid(m) => Status::invalid_argument(m),
        }
    }
}

async fn call<T>(
    tx: &mpsc::Sender<node::Request>,
    make: impl FnOnce(node::Reply<T>) -> node::Request,
) -> Result<T, Status> {
    let (reply, rx) = oneshot::channel();
    tx.send(make(reply))
        .await
        .map_err(|_| Status::unavailable("node stopped"))?;
    rx.await
        .map_err(|_| Status::unavailable("node stopped"))?
        .map_err(Status::from)
}

async fn status(tx: &mpsc::Sender<node::Request>) -> Result<Response<pb::StatusResponse>, Status> {
    let (reply, rx) = oneshot::channel();
    tx.send(node::Request::Status(reply))
        .await
        .map_err(|_| Status::unavailable("node stopped"))?;
    Ok(Response::new(
        rx.await.map_err(|_| Status::unavailable("node stopped"))?,
    ))
}

pub struct KvService(pub mpsc::Sender<node::Request>);

#[tonic::async_trait]
impl pb::kv_server::Kv for KvService {
    async fn get(&self, r: Request<pb::GetRequest>) -> Result<Response<pb::GetResponse>, Status> {
        let key = r.into_inner().key;
        let value = call(&self.0, |tx| node::Request::Get(key, tx)).await?;
        Ok(Response::new(pb::GetResponse { value }))
    }

    async fn write(
        &self,
        r: Request<pb::WriteRequest>,
    ) -> Result<Response<pb::WriteResponse>, Status> {
        let w = r.into_inner();
        if w.client == 0 {
            return Err(Status::invalid_argument("client id must be non-zero"));
        }
        let cmd = kv::Command {
            client: w.client,
            seq: w.seq,
            key: w.key,
            value: w.value,
            cas: w.cas,
            expected: w.expected,
        };
        let res = call(&self.0, |tx| node::Request::Write(cmd, tx)).await?;
        Ok(Response::new(pb::WriteResponse {
            ok: res.ok,
            prev: res.prev,
        }))
    }
}

pub struct AdminService(pub mpsc::Sender<node::Request>);

#[tonic::async_trait]
impl pb::admin_server::Admin for AdminService {
    async fn add_member(
        &self,
        r: Request<pb::Member>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let m = r.into_inner();
        let change = ConfChange::Add(Member {
            id: m.id,
            addr: m.addr,
        });
        call(&self.0, |tx| node::Request::Conf(change, tx)).await?;
        status(&self.0).await
    }

    async fn remove_member(
        &self,
        r: Request<pb::RemoveMemberRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let change = ConfChange::Remove(r.into_inner().id);
        call(&self.0, |tx| node::Request::Conf(change, tx)).await?;
        status(&self.0).await
    }

    async fn status(
        &self,
        _: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        status(&self.0).await
    }
}

pub struct RaftService(pub mpsc::Sender<pb::RaftMessage>);

#[tonic::async_trait]
impl pb::raft_server::Raft for RaftService {
    async fn send(
        &self,
        r: Request<Streaming<pb::RaftMessage>>,
    ) -> Result<Response<pb::Empty>, Status> {
        let mut stream = r.into_inner();
        while let Some(m) = stream.message().await? {
            self.0
                .send(m)
                .await
                .map_err(|_| Status::unavailable("node stopped"))?;
        }
        Ok(Response::new(pb::Empty {}))
    }
}
