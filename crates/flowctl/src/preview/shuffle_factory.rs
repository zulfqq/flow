//! Preview's shuffle-session factory: the choice between a fixture replay
//! (`--fixture`) and a live in-process journal-reading shuffle Session.
//!
//! The [`ShuffleSessionFactory`] seam is monomorphized (`open` /
//! `recv_checkpoint` are `-> impl Future` and `close` takes `self`, so it is
//! not object-safe); this enum lets one leader `Service` host either source,
//! chosen per run. The fixture half is the shared channel-fed opener from
//! `runtime_harness::drive::segments`; the live half reads real journals via a
//! loopback `shuffle::Service`.

use runtime_harness::drive::segments;
use runtime_next::{ShuffleSession, ShuffleSessionFactory};

pub(crate) enum PreviewShuffleFactory {
    Fixture(segments::FixtureOpener),
    Live(runtime_next::ShuffleServiceFactory),
}

impl ShuffleSessionFactory for PreviewShuffleFactory {
    type Session = PreviewShuffleSession;

    async fn open(
        &self,
        task: shuffle::proto::Task,
        shards: Vec<shuffle::proto::Shard>,
        resume: shuffle::Frontier,
    ) -> anyhow::Result<PreviewShuffleSession> {
        Ok(match self {
            Self::Fixture(f) => PreviewShuffleSession::Fixture(f.open(task, shards, resume).await?),
            Self::Live(f) => PreviewShuffleSession::Live(f.open(task, shards, resume).await?),
        })
    }
}

/// Per-session shuffle source opened by [`PreviewShuffleFactory`].
pub(crate) enum PreviewShuffleSession {
    Fixture(segments::FixtureCheckpoints),
    Live(shuffle::SessionClient),
}

impl ShuffleSession for PreviewShuffleSession {
    fn request_checkpoint(&self) {
        match self {
            Self::Fixture(s) => s.request_checkpoint(),
            Self::Live(s) => s.request_checkpoint(),
        }
    }

    async fn recv_checkpoint(&mut self) -> anyhow::Result<shuffle::Frontier> {
        match self {
            Self::Fixture(s) => s.recv_checkpoint().await,
            Self::Live(s) => s.recv_checkpoint().await,
        }
    }

    async fn close(self) -> anyhow::Result<()> {
        match self {
            Self::Fixture(s) => s.close().await,
            Self::Live(s) => s.close().await,
        }
    }
}
