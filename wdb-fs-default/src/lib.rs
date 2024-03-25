use wdb_core::FS;

pub struct DefaultFSController {}

impl DefaultFSController {
    pub fn init() -> DefaultFSController{
        DefaultFSController { }
    }
}

impl FS for DefaultFSController {

}

pub fn add(left: usize, right: usize) -> usize {
    left + right
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
