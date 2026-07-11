use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable generation identity. A new value is required for every startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct GenerationId(pub u128);

/// Lifecycle states visible to the deterministic kernel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LifecycleState {
    Absent,
    Preparing,
    Published,
    Ready,
    Quiescing,
    Stopping,
    Unhealthy,
}

impl LifecycleState {
    /// Apply one state transition. Repair and rollback both return to `Absent`.
    pub fn transition(self, next: Self) -> Result<Self, TransitionError> {
        use LifecycleState::*;
        let legal = matches!(
            (self, next),
            (Absent, Preparing)
                | (Preparing, Published | Absent | Unhealthy)
                | (Published, Ready | Absent | Unhealthy)
                | (Ready, Quiescing | Stopping | Unhealthy)
                | (Quiescing, Ready | Stopping | Unhealthy)
                | (Stopping, Absent | Unhealthy)
                | (Unhealthy, Absent)
        );
        legal.then_some(next).ok_or(TransitionError {
            from: self,
            to: next,
        })
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("illegal lifecycle transition from {from:?} to {to:?}")]
pub struct TransitionError {
    pub from: LifecycleState,
    pub to: LifecycleState,
}

#[cfg(test)]
mod tests {
    use super::LifecycleState::*;

    #[test]
    fn complete_happy_path_is_legal() {
        let mut state = Absent;
        for next in [Preparing, Published, Ready, Quiescing, Stopping, Absent] {
            state = state.transition(next).unwrap();
        }
    }

    #[test]
    fn serving_before_commit_is_illegal() {
        assert!(Published.transition(Quiescing).is_err());
        assert!(Preparing.transition(Ready).is_err());
    }

    #[test]
    fn every_state_pair_matches_the_declared_transition_graph() {
        let states = [
            Absent, Preparing, Published, Ready, Quiescing, Stopping, Unhealthy,
        ];
        let legal = [
            (Absent, Preparing),
            (Preparing, Published),
            (Preparing, Absent),
            (Preparing, Unhealthy),
            (Published, Ready),
            (Published, Absent),
            (Published, Unhealthy),
            (Ready, Quiescing),
            (Ready, Stopping),
            (Ready, Unhealthy),
            (Quiescing, Ready),
            (Quiescing, Stopping),
            (Quiescing, Unhealthy),
            (Stopping, Absent),
            (Stopping, Unhealthy),
            (Unhealthy, Absent),
        ];
        for from in states {
            for to in states {
                assert_eq!(
                    from.transition(to).is_ok(),
                    legal.contains(&(from, to)),
                    "unexpected transition result for {from:?} -> {to:?}"
                );
            }
        }
    }
}
