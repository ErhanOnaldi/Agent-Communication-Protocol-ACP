use acp_protocol::{
    CapabilityScoreRecord, ModelRecord, RuntimeType, SchedulerProfile, WorkflowSlot,
};

#[derive(Debug, Clone)]
pub struct Scheduler {
    pub(crate) models: Vec<ModelRecord>,
    capability_scores: Vec<CapabilityScoreRecord>,
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub role: String,
    pub runtime_type: RuntimeType,
    pub model_id: Option<String>,
    pub score: f64,
}

impl Scheduler {
    pub fn new(models: Vec<ModelRecord>) -> Self {
        Self {
            models,
            capability_scores: Vec::new(),
        }
    }

    pub fn with_scores(mut self, scores: Vec<CapabilityScoreRecord>) -> Self {
        self.capability_scores = scores;
        self
    }

    pub fn assign(
        &self,
        role: &str,
        slot: &WorkflowSlot,
        profile: SchedulerProfile,
    ) -> anyhow::Result<Assignment> {
        let mut best = None;
        for preference in &slot.preferred {
            let model = preference
                .model
                .as_ref()
                .and_then(|id| self.models.iter().find(|m| m.id == *id));
            let score = self.score(slot, preference.runtime, model, profile);
            if best.as_ref().is_none_or(|a: &Assignment| score > a.score) {
                best = Some(Assignment {
                    role: role.to_string(),
                    runtime_type: preference.runtime,
                    model_id: preference.model.clone(),
                    score,
                });
            }
        }
        best.or_else(|| {
            self.models.first().map(|m| Assignment {
                role: role.to_string(),
                runtime_type: m.runtime_source.parse().unwrap_or(RuntimeType::ClaudeCode),
                model_id: Some(m.id.clone()),
                score: 0.5,
            })
        })
        .ok_or_else(|| anyhow::anyhow!("no runtime candidates available for role {role}"))
    }

    pub fn candidates(
        &self,
        role: &str,
        slot: &WorkflowSlot,
        profile: SchedulerProfile,
    ) -> anyhow::Result<Vec<Assignment>> {
        let mut candidates = Vec::new();
        for preference in &slot.preferred {
            let model = preference
                .model
                .as_ref()
                .and_then(|id| self.models.iter().find(|m| m.id == *id));
            candidates.push(Assignment {
                role: role.to_string(),
                runtime_type: preference.runtime,
                model_id: preference.model.clone(),
                score: self.score(slot, preference.runtime, model, profile),
            });
        }
        if candidates.is_empty() {
            candidates.push(self.assign(role, slot, profile)?);
        }
        candidates.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(candidates)
    }

    pub(crate) fn score(
        &self,
        slot: &WorkflowSlot,
        runtime_type: RuntimeType,
        model: Option<&ModelRecord>,
        profile: SchedulerProfile,
    ) -> f64 {
        let capability_match = if slot.required_capabilities.is_empty() {
            1.0
        } else {
            0.75
        };
        let runtime_quality = match runtime_type {
            RuntimeType::ClaudeCode | RuntimeType::Codex => 1.0,
            RuntimeType::Gemini | RuntimeType::Copilot => 0.75,
            RuntimeType::Claudex => 0.65,
        };
        let cost_efficiency = model
            .map(|m| match m.tier.to_string().as_str() {
                "free" | "local" => 1.0,
                "cheap" => 0.85,
                "standard" => 0.65,
                "premium" => 0.35,
                _ => 0.5,
            })
            .unwrap_or(0.5);
        let context_fit = model
            .and_then(|m| m.context_window)
            .map(|w| if w >= 128_000 { 1.0 } else { 0.75 })
            .unwrap_or(0.7);
        let latency = if runtime_type == RuntimeType::Claudex {
            0.8
        } else {
            0.7
        };
        let mut score = (capability_match * 0.30)
            + (runtime_quality * 0.25)
            + (cost_efficiency * 0.20)
            + (context_fit * 0.15)
            + (latency * 0.10);
        match profile {
            SchedulerProfile::BudgetFirst => score += cost_efficiency * 0.10,
            SchedulerProfile::SpeedFirst => score += latency * 0.10,
            SchedulerProfile::QualityFirst => score += runtime_quality * 0.10,
        }
        if let Some(model) = model {
            score += self.learned_boost(runtime_type, &model.id, &slot.required_capabilities);
        }
        score
    }

    fn learned_boost(
        &self,
        runtime_type: RuntimeType,
        model_id: &str,
        capabilities: &[String],
    ) -> f64 {
        if capabilities.is_empty() || self.capability_scores.is_empty() {
            return 0.0;
        }
        let mut total = 0.0;
        let mut count = 0usize;
        for cap in capabilities {
            if let Some(rec) = self.capability_scores.iter().find(|s| {
                s.runtime_type == runtime_type && s.model_id == model_id && s.capability == *cap
            }) {
                let n = rec.success_count + rec.failure_count;
                if n >= 5 {
                    let rate = rec.success_count as f64 / n as f64;
                    // Maps success_rate [0,1] -> learned adjustment [-0.10, +0.10]
                    total += (rate - 0.5) * 0.20;
                    count += 1;
                }
            }
        }
        if count > 0 {
            total / count as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use acp_protocol::{CapabilityScoreRecord, ModelPricing, ModelTier, WorkflowSlot};

    use super::*;

    #[test]
    fn adaptive_score_boosts_high_success_rate() {
        let model = ModelRecord {
            id: "codex/default".to_string(),
            name: "Codex".to_string(),
            runtime_source: "codex".to_string(),
            tier: ModelTier::Premium,
            context_window: None,
            pricing: ModelPricing {
                input: None,
                output: None,
            },
        };
        let scores = vec![CapabilityScoreRecord {
            runtime_type: RuntimeType::Codex,
            model_id: "codex/default".to_string(),
            capability: "rust".to_string(),
            success_count: 9,
            failure_count: 1,
        }];
        let scheduler = Scheduler::new(vec![model]).with_scores(scores);
        let slot = WorkflowSlot {
            role: "backend".to_string(),
            runtime_mode: None,
            preferred: vec![],
            required_capabilities: vec!["rust".to_string()],
            optional: false,
        };
        let base = scheduler.score(&slot, RuntimeType::Codex, None, SchedulerProfile::QualityFirst);
        let with_model = scheduler.score(
            &slot,
            RuntimeType::Codex,
            scheduler.models.first(),
            SchedulerProfile::QualityFirst,
        );
        assert!(with_model > base);
    }
}
