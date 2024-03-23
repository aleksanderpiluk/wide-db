pub mod app_controller;


pub trait StorageEngine {
    
}

pub trait FS {
    
}

pub trait Module {
    fn init(&self);
    fn destoy(&self);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
