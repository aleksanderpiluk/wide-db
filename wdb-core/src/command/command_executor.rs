use std::sync::Arc;

use tokio::task::JoinError;

use crate::storage_engine::StorageEngine;

use super::{command::Command, command_context::CommandContext, command_error::CommandError};

#[derive(Debug)]
pub struct CommandExecutor {
    pub storage_engine: Arc<dyn StorageEngine>,
}

impl CommandExecutor {
    pub async fn exec_command<T: Send + Sync + 'static, C: Command<T> + 'static>(&self, cmd: C) -> Result<Result<T, CommandError>, JoinError> {
        let storage_engine = self.storage_engine.clone();

        let handle = tokio::spawn(async move {
            cmd.execute(CommandContext {
                storage_engine,
            })
        });

        handle.await
    }
}