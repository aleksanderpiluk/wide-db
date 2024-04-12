use crate::command::{command::Command, command_context::CommandContext, command_error::CommandError};

pub struct CommandCreateTable {
    pub name: String,
    pub families: Vec<String>,
}

impl Command<()> for CommandCreateTable {
    fn execute(&self, ctx: CommandContext) -> Result<(), CommandError> {
        self.validate_params()?;

        match self.execute(&ctx) {
            Err(err) => Err(CommandError::ExecutionError(err)),
            Ok(_) => Ok(()),  
        }?;
        
        Ok(())
    }
}

impl CommandCreateTable {
    fn validate_params(&self) -> Result<(), CommandError> {
        println!("Validating params...");
        println!("name: {} , families: {:?}", self.name, self.families);

        if self.name.len() <= 0 {
            return Err(CommandError::InputValidadationError("Invalid name. Name cannot be an empty string."));
        }

        for family in &self.families {
            if family.len() <= 0 {
                return Err(CommandError::InputValidadationError("Invalid family name. Family name cannot be an empty string."));
            }
        }

        Ok(())
    }

    fn execute(&self, ctx: &CommandContext) -> Result<(), &'static str> { 
        ctx.storage_engine.create_table(&self.name, &self.families)?;

        Ok(())
    }
}