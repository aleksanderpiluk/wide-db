pub enum CommandError {
    InputValidadationError(&'static str),
    ExecutionError(&'static str),
}