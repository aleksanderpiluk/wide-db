use uuid::Uuid;

pub type TableId = String;
pub struct TableIdGen {}

impl TableIdGen {
    pub fn gen_id() -> TableId {
        Uuid::now_v7().to_string()
    }
}