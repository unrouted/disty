use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone)]
pub struct Clock {
    inner: Inner,
}

#[derive(Debug, Clone)]
enum Inner {
    Realtime,
    #[cfg(test)]
    Mocked {
        start: DateTime<Utc>,
        offset: std::sync::Arc<std::sync::Mutex<Duration>>, // interior mutability
    },
}

impl Clock {
    /// Creates a new real-time clock.
    pub fn new() -> Self {
        Self {
            inner: Inner::Realtime,
        }
    }

    /// Returns the current time.
    pub fn now(&self) -> DateTime<Utc> {
        match &self.inner {
            Inner::Realtime => Utc::now(),
            #[cfg(test)]
            Inner::Mocked { start, offset } => *start + *offset.lock().unwrap(),
        }
    }

    /// Creates a mocked clock for testing.
    #[cfg(test)]
    pub fn mocked(start: DateTime<Utc>) -> Self {
        Self {
            inner: Inner::Mocked {
                start,
                offset: std::sync::Arc::new(std::sync::Mutex::new(Duration::zero())),
            },
        }
    }

    /// Advances the mocked clock. No-op in real-time mode.
    #[cfg(test)]
    pub fn advance(&self, duration: Duration) {
        if let Inner::Mocked { offset, .. } = &self.inner {
            let mut guard = offset.lock().unwrap();
            *guard = *guard + duration;
        }
    }

    /// Returns the current offset. Mostly for test assertions.
    #[cfg(test)]
    pub fn offset(&self) -> Option<Duration> {
        match &self.inner {
            Inner::Realtime => None,
            Inner::Mocked { offset, .. } => Some(*offset.lock().unwrap()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};

    fn test_start() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
    }

    #[test]
    fn mocked_clock_advances_with_shared_ref() {
        let clock = Clock::mocked(test_start());
        assert_eq!(clock.now(), test_start());

        clock.advance(Duration::seconds(42));
        assert_eq!(clock.now(), test_start() + Duration::seconds(42));
    }

    #[test]
    fn offset_is_correct_after_advancing() {
        let clock = Clock::mocked(test_start());
        clock.advance(Duration::minutes(5));
        assert_eq!(clock.offset(), Some(Duration::minutes(5)));
    }

    #[test]
    fn advance_does_nothing_for_realtime() {
        let clock = Clock::new();
        clock.advance(Duration::seconds(100)); // Should not panic or do anything
        assert!(clock.offset().is_none());
    }

    #[test]
    fn real_clock_is_close_to_now() {
        let clock = Clock::new();
        let delta = (clock.now() - Utc::now()).num_seconds().abs();
        assert!(delta < 2, "Real clock differs by too much: {delta}s");
    }
}
