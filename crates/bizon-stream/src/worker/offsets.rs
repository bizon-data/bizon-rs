//! Which offsets may be committed: per partition, the next offset after the longest prefix of
//! consumed messages whose rows are acknowledged (or that were skipped). Revoking a partition bumps
//! its generation so acks for rows read before the revoke cannot advance the new owner's state.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Tp {
    pub topic: Arc<str>,
    pub partition: i32,
}

#[derive(Debug, Clone)]
pub struct Ticket {
    pub tp: Tp,
    generation: u64,
    offset: i64,
}

#[derive(Debug)]
struct Partition {
    generation: u64,
    /// Consumed, not yet committable offsets in consume order, with their done flag and when they
    /// were consumed.
    pending: VecDeque<(i64, bool, Instant)>,
    /// Next offset to commit (last contiguous done + 1), and the last one committed.
    committable: Option<i64>,
    committed: Option<i64>,
}

#[derive(Debug, Default)]
pub struct Tracker {
    parts: HashMap<Tp, Partition>,
    next_generation: u64,
}

impl Tracker {
    pub fn assign(&mut self, tp: Tp) {
        self.next_generation += 1;
        let generation = self.next_generation;
        self.parts.entry(tp).or_insert(Partition {
            generation,
            pending: VecDeque::new(),
            committable: None,
            committed: None,
        });
    }

    pub fn revoke(&mut self, tp: &Tp) {
        self.parts.remove(tp);
    }

    pub fn assigned(&self) -> impl Iterator<Item = &Tp> {
        self.parts.keys()
    }

    /// Registers a consumed offset; `None` if the partition is not assigned (it was just revoked).
    pub fn track(&mut self, tp: &Tp, offset: i64) -> Option<Ticket> {
        let p = self.parts.get_mut(tp)?;
        p.pending.push_back((offset, false, Instant::now()));
        Some(Ticket {
            tp: tp.clone(),
            generation: p.generation,
            offset,
        })
    }

    pub fn done(&mut self, t: &Ticket) {
        let Some(p) = self.parts.get_mut(&t.tp).filter(|p| p.generation == t.generation) else {
            return;
        };
        if let Ok(i) = p.pending.binary_search_by_key(&t.offset, |(o, _, _)| *o) {
            p.pending[i].1 = true;
        }
        while let Some(&(o, true, _)) = p.pending.front() {
            p.pending.pop_front();
            p.committable = Some(o + 1);
        }
    }

    /// Offsets that advanced since the last call, marked as committed.
    pub fn take_commits(&mut self, only: Option<&[Tp]>) -> Vec<(Tp, i64)> {
        let mut out = Vec::new();
        for (tp, p) in self.parts.iter_mut() {
            if only.is_some_and(|o| !o.contains(tp)) {
                continue;
            }
            if let Some(c) = p.committable.filter(|c| Some(*c) != p.committed) {
                p.committed = Some(c);
                out.push((tp.clone(), c));
            }
        }
        out.sort();
        out
    }

    /// When the oldest consumed, not yet acknowledged message was consumed. A partition's front entry
    /// is never done (done fronts are popped), so it is that partition's oldest.
    pub fn oldest_pending(&self) -> Option<Instant> {
        self.parts.values().filter_map(|p| p.pending.front().map(|e| e.2)).min()
    }

    /// Offsets to commit again for partitions with nothing in flight, so the broker never expires them:
    /// a quiet partition is otherwise committed once and re-read from `auto.offset.reset` once its
    /// commit ages out while no member of the group subscribes to its topic. Partitions this worker
    /// has not committed yet are listed separately; their broker offset must be read first.
    pub fn idle_commits(&self) -> (Vec<(Tp, i64)>, Vec<Tp>) {
        let (mut known, mut unknown) = (Vec::new(), Vec::new());
        for (tp, p) in &self.parts {
            if !p.pending.is_empty() || p.committable != p.committed {
                continue;
            }
            match p.committed {
                Some(c) => known.push((tp.clone(), c)),
                None => unknown.push(tp.clone()),
            }
        }
        known.sort();
        unknown.sort();
        (known, unknown)
    }

    /// Takes the broker's committed offset for a partition this worker has neither consumed from nor
    /// committed since assignment. Returns false if that is no longer true.
    pub fn adopt_committed(&mut self, tp: &Tp, offset: i64) -> bool {
        match self.parts.get_mut(tp) {
            Some(p) if p.pending.is_empty() && p.committable.is_none() && p.committed.is_none() => {
                p.committable = Some(offset);
                p.committed = Some(offset);
                true
            }
            _ => false,
        }
    }

    /// Consumed offsets not yet acknowledged, for the given partitions (all when `None`).
    pub fn in_flight(&self, only: Option<&[Tp]>) -> usize {
        self.parts
            .iter()
            .filter(|(tp, _)| only.is_none_or(|o| o.contains(tp)))
            .map(|(_, p)| p.pending.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(p: i32) -> Tp {
        Tp {
            topic: "t".into(),
            partition: p,
        }
    }

    #[test]
    fn commits_only_contiguous_acked_prefix() {
        let mut t = Tracker::default();
        t.assign(tp(0));
        let a: Vec<_> = (10..15).map(|o| t.track(&tp(0), o).unwrap()).collect();
        t.done(&a[1]);
        t.done(&a[2]);
        assert!(t.take_commits(None).is_empty(), "offset 10 is still pending");
        let oldest = t.oldest_pending().unwrap();
        t.done(&a[0]);
        assert!(t.oldest_pending().unwrap() >= oldest, "the oldest moved to offset 13");
        assert_eq!(t.take_commits(None), vec![(tp(0), 13)]);
        assert!(t.take_commits(None).is_empty(), "nothing new");
        t.done(&a[4]);
        t.done(&a[3]);
        assert_eq!(t.take_commits(None), vec![(tp(0), 15)]);
        assert_eq!(t.in_flight(None), 0);
        assert_eq!(t.oldest_pending(), None);
    }

    #[test]
    fn late_acks_after_revoke_are_ignored() {
        let mut t = Tracker::default();
        t.assign(tp(0));
        let old = t.track(&tp(0), 5).unwrap();
        t.revoke(&tp(0));
        assert!(t.track(&tp(0), 6).is_none());
        t.assign(tp(0));
        let new = t.track(&tp(0), 5).unwrap();
        t.done(&old);
        assert_eq!(t.in_flight(None), 1, "the stale ack did not complete the new generation's offset");
        t.done(&new);
        assert_eq!(t.take_commits(None), vec![(tp(0), 6)]);
    }

    #[test]
    fn idle_partitions_are_recommitted_busy_ones_are_not() {
        let mut t = Tracker::default();
        for p in 0..4 {
            t.assign(tp(p));
        }
        let a = t.track(&tp(0), 10).unwrap();
        t.done(&a);
        assert_eq!(t.take_commits(None), vec![(tp(0), 11)]);
        t.track(&tp(1), 5).unwrap();
        let c = t.track(&tp(2), 7).unwrap();
        t.done(&c);
        // tp0: committed and idle; tp1: in flight; tp2: acked but not committed yet; tp3: never consumed.
        assert_eq!(t.idle_commits(), (vec![(tp(0), 11)], vec![tp(3)]));

        assert!(t.adopt_committed(&tp(3), 42));
        assert!(!t.adopt_committed(&tp(3), 50), "already known");
        assert!(!t.adopt_committed(&tp(1), 1), "has messages in flight");
        assert!(t.take_commits(None).contains(&(tp(2), 8)));
        assert_eq!(t.idle_commits(), (vec![(tp(0), 11), (tp(2), 8), (tp(3), 42)], vec![]));
        assert!(t.take_commits(None).is_empty(), "adopted offsets are not new commits");
    }

    #[test]
    fn scoped_commits_and_in_flight() {
        let mut t = Tracker::default();
        t.assign(tp(0));
        t.assign(tp(1));
        let a = t.track(&tp(0), 1).unwrap();
        let _b = t.track(&tp(1), 1).unwrap();
        t.done(&a);
        assert_eq!(t.in_flight(Some(&[tp(1)])), 1);
        assert_eq!(t.take_commits(Some(&[tp(1)])), vec![]);
        assert_eq!(t.take_commits(Some(&[tp(0)])), vec![(tp(0), 2)]);
    }
}
