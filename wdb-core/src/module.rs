use std::sync::Arc;

use crate::command::command_executor::CommandExecutor;

pub trait Module {
    fn init(&self, cmd_exec: Arc<CommandExecutor>);
    fn destoy(&self);
}