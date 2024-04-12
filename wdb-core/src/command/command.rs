use super::{command_context::CommandContext, command_error::CommandError};

pub trait Command<T: Send + Sync>: Send + Sync {
    fn execute(&self, ctx: CommandContext) -> Result<T, CommandError>;
}