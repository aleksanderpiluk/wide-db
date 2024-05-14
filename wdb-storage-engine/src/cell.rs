pub trait Cell {
    fn get_key_len(&self) -> u16;
    fn get_value_len(&self) -> u64;
    fn get_row_len(&self) -> u16;
    fn get_row(&self) -> &[u8];
    fn get_cf_len(&self) -> u16;
    fn get_cf(&self) -> &[u8];
    fn get_col_len(&self) -> u16;
    fn get_col(&self) -> &[u8];
    fn get_timestamp(&self) -> u64;
    fn get_cell_type(&self) -> CellType;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum CellType {
    Minimum = 0,
    Put = 4,
    Delete = 8,
    Maximum = 255,
}

impl TryFrom<u8> for CellType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CellType::Minimum),
            4 => Ok(CellType::Put),
            8 => Ok(CellType::Delete),
            255 => Ok(CellType::Maximum),
            _ => Err("Invalid value trying to convert u8 to CellType enum.")
        }
    }
}