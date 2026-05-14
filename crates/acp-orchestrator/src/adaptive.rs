use acp_protocol::{RuntimeHealth, SchedulerProfile};

/// Monitors pipeline health and automatically adjusts the scheduler profile.
///
/// Switches from QualityFirst → SpeedFirst when the failure rate exceeds 30%
/// after at least 4 steps, reducing the chance of further quality-model failures.
pub struct AdaptiveController {
    failure_count: u32,
    step_count: u32,
    pub profile: SchedulerProfile,
}

impl AdaptiveController {
    pub fn new(initial: SchedulerProfile) -> Self {
        Self {
            failure_count: 0,
            step_count: 0,
            profile: initial,
        }
    }

    /// Record a completed step's health and return the (possibly updated) profile.
    pub fn record_step(&mut self, health: RuntimeHealth) -> SchedulerProfile {
        self.step_count += 1;
        if health != RuntimeHealth::Healthy {
            self.failure_count += 1;
        }
        if self.profile == SchedulerProfile::QualityFirst
            && self.step_count >= 4
            && self.failure_count as f64 / self.step_count as f64 > 0.30
        {
            tracing::info!(
                failure_count = self.failure_count,
                step_count = self.step_count,
                "adaptive: switching profile quality_first → speed_first due to high failure rate"
            );
            self.profile = SchedulerProfile::SpeedFirst;
        }
        self.profile
    }

    pub fn failure_rate(&self) -> f64 {
        if self.step_count == 0 {
            return 0.0;
        }
        self.failure_count as f64 / self.step_count as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_switch_below_threshold() {
        let mut ctrl = AdaptiveController::new(SchedulerProfile::QualityFirst);
        for _ in 0..4 {
            ctrl.record_step(RuntimeHealth::Healthy);
        }
        assert_eq!(ctrl.profile, SchedulerProfile::QualityFirst);
    }

    #[test]
    fn switches_profile_at_high_failure_rate() {
        let mut ctrl = AdaptiveController::new(SchedulerProfile::QualityFirst);
        ctrl.record_step(RuntimeHealth::Healthy);
        ctrl.record_step(RuntimeHealth::RateLimited);
        ctrl.record_step(RuntimeHealth::Crashed);
        let profile = ctrl.record_step(RuntimeHealth::RateLimited);
        // 3/4 = 75% failure rate → switch
        assert_eq!(profile, SchedulerProfile::SpeedFirst);
    }

    #[test]
    fn budget_first_profile_not_changed() {
        let mut ctrl = AdaptiveController::new(SchedulerProfile::BudgetFirst);
        for _ in 0..8 {
            ctrl.record_step(RuntimeHealth::Crashed);
        }
        assert_eq!(ctrl.profile, SchedulerProfile::BudgetFirst);
    }
}
