use bytes::Bytes;

#[derive(Debug)]
pub struct TableFamily {
    id: u64,
    name: Bytes,
    sstables: Vec<()>,
}

impl TableFamily {
    pub fn new(id: u64, name: Bytes) -> TableFamily {
        TableFamily { id, name, sstables: Vec::new() }
    }

    // pub fn insert_cell(&self, cell: Cell) {
    //     self.memtable.insert(cell);
    // }

    // pub fn flush(&mut self) {
    //     self.memtable.snapshot();
    // }

    // pub fn read_row(&self, row: &Bytes) {
    //     self.memtable.read_row(row);
    // }
}