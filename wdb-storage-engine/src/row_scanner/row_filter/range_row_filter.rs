use super::row_filter::RowFilter;

pub struct RangeRowFilter {
    pub start: String,
    pub end: String,
}

impl RowFilter for RangeRowFilter {
    fn check(&self, row: String) -> bool {
        return self.start <= row && row <= self.end;
    }
}