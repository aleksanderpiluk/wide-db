use super::row_filter::RowFilter;

pub struct PrefixRowFilter {
    pub prefix: String,
}

impl RowFilter for PrefixRowFilter {
    fn check(&self, row: String) -> bool {
        return row.starts_with(self.prefix.as_str());
    }
}