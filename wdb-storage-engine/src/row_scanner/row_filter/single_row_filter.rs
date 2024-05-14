use super::row_filter::RowFilter;

pub struct SingleRowFilter {
    pub row: String,
}

impl RowFilter for SingleRowFilter {
    fn check(&self, row: String) -> bool {
        return self.row == row;
    }
} 