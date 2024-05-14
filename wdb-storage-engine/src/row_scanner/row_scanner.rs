use super::row_filter::row_filter::RowFilter;

pub struct RowScanner {
    pub row_filter: dyn RowFilter, 
}