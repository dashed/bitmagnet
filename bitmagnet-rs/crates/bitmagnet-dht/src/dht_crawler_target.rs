use std::sync::{Arc, PoisonError, RwLock};

use crate::Id20;

/// Cloneable node-ID target shared by DHT crawler query workers.
///
/// This handle stores only the current target. It does not generate IDs,
/// rotate the value, start a task, or own a timer. Reads and module-owned
/// replacements synchronize the entire 20-byte ID, so callers never observe a
/// torn value. A poisoned lock is recovered because the protected invariant is
/// only one copyable [`Id20`] value.
#[derive(Clone)]
pub struct DhtCrawlerTarget {
    inner: Arc<RwLock<Id20>>,
}

impl DhtCrawlerTarget {
    /// Construct a shared target with an explicit initial node ID.
    #[must_use]
    pub fn new(initial: Id20) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial)),
        }
    }

    /// Read one stable snapshot of the current target.
    #[must_use]
    pub fn current(&self) -> Id20 {
        *self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Replace the target observed by every clone.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "consumed by the future target rotator slice")
    )]
    fn set(&self, next: Id20) {
        *self.inner.write().unwrap_or_else(PoisonError::into_inner) = next;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use super::*;

    fn id(value: u8) -> Id20 {
        let mut bytes = [0; 20];
        bytes[19] = value;
        Id20::from_slice(&bytes).unwrap()
    }

    #[test]
    fn clones_observe_module_owned_replacements() {
        let target = DhtCrawlerTarget::new(id(1));
        let clone = target.clone();

        target.set(id(2));
        assert_eq!(clone.current(), id(2));
    }

    #[test]
    fn handle_is_send_sync_and_concurrent_reads_are_whole_ids() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DhtCrawlerTarget>();

        let first = Id20::from_slice(&[0x00; 20]).unwrap();
        let second = Id20::from_slice(&[0xff; 20]).unwrap();
        let target = DhtCrawlerTarget::new(first);
        let start = Arc::new(Barrier::new(5));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let reader = target.clone();
            let reader_start = start.clone();
            readers.push(std::thread::spawn(move || {
                reader_start.wait();
                for _ in 0..10_000 {
                    assert!(matches!(reader.current(), value if value == first || value == second));
                }
            }));
        }

        start.wait();
        for index in 0..10_000 {
            target.set(if index % 2 == 0 { second } else { first });
        }
        for reader in readers {
            reader.join().unwrap();
        }

        target.set(second);
        let observer = target.clone();
        assert_eq!(observer.current(), second);
    }

    #[test]
    fn returned_snapshot_remains_stable_after_replacement() {
        let target = DhtCrawlerTarget::new(id(1));
        let snapshot = target.current();

        target.set(id(2));

        assert_eq!(snapshot, id(1));
        assert_eq!(target.current(), id(2));
    }

    #[test]
    fn zero_is_an_accepted_explicit_target() {
        let target = DhtCrawlerTarget::new(Id20::ZERO);

        assert_eq!(target.current(), Id20::ZERO);
    }

    #[test]
    fn poisoned_writer_does_not_prevent_reads_or_replacement() {
        let target = DhtCrawlerTarget::new(id(1));
        let poisoner = target.clone();

        let panic = std::thread::spawn(move || {
            let _guard = poisoner.inner.write().unwrap();
            panic!("poison target lock");
        })
        .join();
        assert!(panic.is_err());

        assert_eq!(target.current(), id(1));
        target.set(id(2));
        assert_eq!(target.current(), id(2));
    }
}
