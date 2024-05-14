use crate::command::{command::Command, command_context::CommandContext, command_error::CommandError};

pub struct CommandGetRow {
    pub table: String,
    pub row: String,
}

impl Command<()> for CommandGetRow {
    fn execute(&self, ctx: CommandContext) -> Result<(), CommandError> {
        self.validate_params()?;

        match self.execute(&ctx) {
            Err(err) => Err(CommandError::ExecutionError(err)),
            Ok(_) => Ok(()),  
        }?;
        
        Ok(())
    }
}

impl CommandGetRow {
    fn validate_params(&self) -> Result<(), CommandError> {
        if self.table.len() <= 0 {
            return Err(CommandError::InputValidadationError("Invalid table name. Table name cannot be an empty string."));
        }

        if self.row.len() <= 0 {
            return Err(CommandError::InputValidadationError("Invalid row name. Row name cannot be an empty string."));
        }

        Ok(())
    }

    fn execute(&self, ctx: &CommandContext) -> Result<(), &'static str> { 
        ctx.storage_engine.get_row(&self.table, &self.row)?;

        Ok(())
    }
}