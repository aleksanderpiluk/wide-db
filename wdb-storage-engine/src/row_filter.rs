pub trait RowFilter {
    fn check(&self, row: String) -> bool;
}