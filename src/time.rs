use chrono::{DateTime, Utc};

pub fn now() -> DateTime<Utc> {
    real_now()
}

#[cfg(not(test))]
fn real_now() -> DateTime<Utc> {
    Utc::now()
}

#[cfg(test)]
pub mod fake_time {
    use chrono::{DateTime, Duration, Utc};
    use std::sync::{Mutex, OnceLock};
    use tokio::time::Instant;

    pub struct FakeClock {
        anchor_wall: DateTime<Utc>,
        anchor_instant: Instant,
    }

    impl FakeClock {
        pub fn new() -> Self {
            Self {
                anchor_wall: Utc::now(),
                anchor_instant: Instant::now(),
            }
        }

        pub fn now(&self) -> DateTime<Utc> {
            let elapsed = Instant::now().duration_since(self.anchor_instant);
            self.anchor_wall + Duration::from_std(elapsed).unwrap()
        }
    }

    static CLOCK: OnceLock<Mutex<FakeClock>> = OnceLock::new();

    pub fn install_fake_clock() {
        CLOCK.set(Mutex::new(FakeClock::new())).ok();
    }

    pub fn get_now() -> DateTime<Utc> {
        CLOCK
            .get()
            .expect("FakeClock not initialized — call install_fake_clock()")
            .lock()
            .unwrap()
            .now()
    }
}

#[cfg(test)]
fn real_now() -> DateTime<Utc> {
    fake_time::get_now()
}
