//! Which offsets may be committed: per partition, the next offset after the longest prefix of
//! consumed messages whose rows are acknowledged (or that were skipped). Revoking a partition bumps
//! its generation so acks for rows read before the revoke cannot advance the new owner's state.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

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
    /// Consumed, not yet committable offsets in consume order, with their done flag.
    pending: VecDeque<(i64, bool)>,
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
        p.pending.push_back((offset, false));
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
        if let Ok(i) = p.pending.binary_search_by_key(&t.offset, |(o, _)| *o) {
            p.pending[i].1 = true;
        }
        while let Some(&(o, true)) = p.pending.front() {
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
        t.done(&a[0]);
        assert_eq!(t.take_commits(None), vec![(tp(0), 13)]);
        assert!(t.take_commits(None).is_empty(), "nothing new");
        t.done(&a[4]);
        t.done(&a[3]);
        assert_eq!(t.take_commits(None), vec![(tp(0), 15)]);
        assert_eq!(t.in_flight(None), 0);
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
