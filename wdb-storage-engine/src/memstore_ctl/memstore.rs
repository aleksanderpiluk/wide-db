use crossbeam_skiplist::SkipSet;

use super::kv::KV;

pub struct Memstore {
    // active: Segment
}

struct Segment {
    id: String,
    data: SkipSet<KV>
}

impl Segment {
    fn insert_kv(&self, kv: KV) {
        self.data.insert(kv);
    }
}