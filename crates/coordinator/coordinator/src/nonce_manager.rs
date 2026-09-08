//! Monotonic nonce reservation for release-time putInbox transaction building.

use std::collections::BTreeSet;
use std::future::Future;

use tokio::sync::Mutex;

use compose_primitives_traits::CoordinatorError;

/// Monotonic nonce manager for coordinator-signed `putInbox` transactions.
#[derive(Debug)]
pub(crate) struct DeferredNonceManager {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    next_nonce: u64,
    initialized: bool,
    /// Nonces that were reserved but whose transaction never reached the
    /// builder. The coordinator signs every `putInbox`/`sendConfirm`/
    /// `sendAbort` on a chain from one account, so a nonce that is handed out
    /// and then dropped leaves a permanent hole: the builder's execution
    /// cursor stops there and every later coordinator transaction — i.e.
    /// every later cross-chain transaction on that chain — is stuck behind
    /// it. Reusing the nonce is the only way to close the hole.
    freed: BTreeSet<u64>,
}

impl DeferredNonceManager {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                next_nonce: 0,
                initialized: false,
                freed: BTreeSet::new(),
            }),
        }
    }

    /// Reserve a contiguous nonce range and return the starting nonce.
    ///
    /// A single-nonce reservation prefers the lowest recycled nonce, so a hole
    /// left by a failed build or a rejected submission is refilled by the next
    /// transaction instead of stalling the sequence.
    pub(crate) async fn reserve<F, Fut>(
        &self,
        count: usize,
        fetch_base: F,
    ) -> Result<u64, CoordinatorError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<u64, CoordinatorError>>,
    {
        if count == 0 {
            return Ok(0);
        }

        let mut inner = self.inner.lock().await;
        if !inner.initialized {
            inner.next_nonce = fetch_base().await?;
            inner.initialized = true;
        }

        // Only single reservations can come from the free list: a multi-nonce
        // reservation must stay contiguous.
        if count == 1 {
            if let Some(&nonce) = inner.freed.iter().next() {
                inner.freed.remove(&nonce);
                return Ok(nonce);
            }
        }

        let start = inner.next_nonce;
        inner.next_nonce = inner.next_nonce.saturating_add(count as u64);
        Ok(start)
    }

    /// Give back a reservation whose transaction was never handed to the
    /// builder, so the next `reserve` reuses it.
    ///
    /// Only safe when the transaction is known not to have been accepted — a
    /// build failure, or a submission the builder explicitly rejected. After
    /// an ambiguous failure (a timeout, say) the transaction may still be in
    /// the builder's pool, so the nonce must stay consumed.
    pub(crate) async fn release(&self, start: u64, count: usize) {
        if count == 0 {
            return;
        }
        let mut inner = self.inner.lock().await;
        let end = start.saturating_add(count as u64);
        if inner.next_nonce == end {
            // Reclaiming the tail keeps the sequence dense without growing
            // the free list.
            inner.next_nonce = start;
            return;
        }
        for nonce in start..end {
            inner.freed.insert(nonce);
        }
    }

    /// Snap the counter to a nonce the builder explicitly asked for, in either
    /// direction, discarding recycled nonces that are no longer valid.
    ///
    /// The only caller is a nonce-gap rejection, where the builder has told us
    /// precisely which nonce it will accept next. That answer supersedes the
    /// local counter — including moving it *down*, which no other path may do,
    /// because the alternative is re-offering a refused nonce forever.
    ///
    /// The free list is discarded wholesale, not filtered: its entries were
    /// derived from the counter the builder just contradicted, and since
    /// `reserve` prefers the lowest freed nonce, keeping any of them would hand
    /// back the very value that was refused. `expected` is by definition the
    /// first nonce the builder will take, so counting up from it covers every
    /// hole below it.
    pub(crate) async fn force_set(&self, nonce: u64) {
        let mut inner = self.inner.lock().await;
        inner.next_nonce = nonce;
        inner.initialized = true;
        inner.freed.clear();
    }

    /// Number of recycled nonces currently waiting to be reused. Exposed for
    /// the liveness dump.
    pub(crate) async fn freed_count(&self) -> usize {
        self.inner.lock().await.freed.len()
    }

    /// Mark the nonce state as uninitialized so the next `reserve` call
    /// will re-fetch the base nonce from the builder.
    ///
    /// Call this when a `resync` fails so that stale nonces are not reused.
    pub(crate) async fn reset(&self) {
        let mut inner = self.inner.lock().await;
        inner.initialized = false;
        inner.next_nonce = 0;
        inner.freed.clear();
    }

    /// Refresh the local counter from the canonical nonce view without moving
    /// behind nonce ranges that were already reserved locally.
    ///
    /// This is the only resync there is, deliberately. `canonical_nonce_at()`
    /// reports the *on-chain* nonce, which knows nothing about the coordinator
    /// transactions already queued in the builder's pool, so assigning it
    /// unconditionally rewinds the counter over nonces that are already in
    /// use — every transaction built afterwards is then rejected for a nonce
    /// gap until the counter climbs back.
    pub(crate) async fn resync_monotonic<F, Fut>(
        &self,
        fetch_base: F,
    ) -> Result<(), CoordinatorError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<u64, CoordinatorError>>,
    {
        let mut inner = self.inner.lock().await;
        let fetched = fetch_base().await?;
        inner.next_nonce = if inner.initialized {
            inner.next_nonce.max(fetched)
        } else {
            fetched
        };
        inner.initialized = true;
        // Anything below the canonical nonce has already executed on chain and
        // can never be reused.
        inner.freed.retain(|&nonce| nonce >= fetched);
        Ok(())
    }
}

impl Default for DeferredNonceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resync_monotonic_does_not_reuse_reserved_nonce_range() {
        let manager = DeferredNonceManager::new();

        assert_eq!(manager.reserve(2, || async { Ok(7) }).await.unwrap(), 7);

        manager.resync_monotonic(|| async { Ok(8) }).await.unwrap();
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 9);

        manager.resync_monotonic(|| async { Ok(12) }).await.unwrap();
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 12);
    }

    #[tokio::test]
    async fn released_tail_reservation_is_handed_out_again() {
        let manager = DeferredNonceManager::new();

        assert_eq!(manager.reserve(1, || async { Ok(7) }).await.unwrap(), 7);
        manager.release(7, 1).await;

        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 7);
        assert_eq!(manager.freed_count().await, 0);
    }

    #[tokio::test]
    async fn released_middle_reservation_refills_the_hole_first() {
        // The failure that froze chain A: a coordinator transaction is built,
        // its nonce is consumed, the submission is rejected, and later
        // transactions have already taken the nonces above it. Recycling the
        // hole is what keeps the coordinator's sequence contiguous.
        let manager = DeferredNonceManager::new();

        assert_eq!(manager.reserve(1, || async { Ok(100) }).await.unwrap(), 100);
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 101);
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 102);

        manager.release(100, 1).await; // rejected; 101/102 already in flight
        assert_eq!(manager.freed_count().await, 1);

        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 100);
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 103);
    }

    #[tokio::test]
    async fn multi_nonce_reservation_stays_contiguous() {
        let manager = DeferredNonceManager::new();

        assert_eq!(manager.reserve(1, || async { Ok(5) }).await.unwrap(), 5);
        manager.release(5, 1).await;

        // A two-nonce reservation must not start from the free list.
        assert_eq!(manager.reserve(2, || async { Ok(0) }).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn force_set_moves_the_counter_down_and_drops_stale_recycled_nonces() {
        // The livelock this exists to break: the counter has run ahead of the
        // builder (a mass abort retracted reservations), and the refused nonce
        // sits in the free list ready to be offered again and refused again.
        let manager = DeferredNonceManager::new();

        assert_eq!(manager.reserve(1, || async { Ok(5155) }).await.unwrap(), 5155);
        manager.release(5155, 1).await;
        assert_eq!(manager.freed_count().await, 0); // tail giveback
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 5155);
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 5156);
        manager.release(5155, 1).await; // refused again, now parked in `freed`
        assert_eq!(manager.freed_count().await, 1);

        // The builder says it wants 5146. That answer wins.
        manager.force_set(5146).await;
        assert_eq!(manager.freed_count().await, 0, "stale recycled nonces must go");
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 5146);
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 5147);
    }

    #[tokio::test]
    async fn resync_monotonic_drops_recycled_nonces_already_on_chain() {
        let manager = DeferredNonceManager::new();

        assert_eq!(manager.reserve(1, || async { Ok(10) }).await.unwrap(), 10);
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 11);
        manager.release(10, 1).await;
        assert_eq!(manager.freed_count().await, 1);

        // Chain moved past 10, so that nonce can never be reused again.
        manager.resync_monotonic(|| async { Ok(12) }).await.unwrap();
        assert_eq!(manager.freed_count().await, 0);
        assert_eq!(manager.reserve(1, || async { Ok(0) }).await.unwrap(), 12);
    }
}
