use rand::Rng;

pub trait RandomGenerator {
    fn range(&self, min: u64, max: u64) -> u64;
}

pub struct RandGen;

impl RandomGenerator for RandGen {
    fn range(&self, min: u64, max: u64) -> u64 {
        let mut rng = rand::rng();
        rng.random_range(min..=max)
    }
}

pub trait Clock {
    fn now(&self) -> tokio::time::Instant;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
    }
}
