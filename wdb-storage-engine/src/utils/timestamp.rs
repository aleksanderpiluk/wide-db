use std::time;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const MIN: Timestamp = Timestamp(u64::MIN);
    pub const MAX: Timestamp = Timestamp(u64::MAX);

    pub fn new(ms: u64) -> Timestamp {
        Timestamp(ms)
    }

    pub fn ensure_timestamp(ts: Option<Timestamp>) -> Timestamp {
        let result = match ts {
            Some(ts) => ts,
            None => {
                let dur = time::SystemTime::now()
                    .duration_since(time::UNIX_EPOCH)
                    .expect("Time went backwards...");

                Timestamp(dur.as_millis() as u64)
            }
        };

        result
    }
}

impl From<u64> for Timestamp {
    fn from(value: u64) -> Self {
        Timestamp(value)
    }
}

impl Into<u64> for Timestamp {
    fn into(self) -> u64 {
        self.0
    }
}