use acp_protocol::{PipelineStatus, RuntimeHealth};
use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame,
};

use super::state::DashboardState;

pub fn draw_dashboard(frame: &mut Frame, state: &DashboardState) {
    let area = frame.area();
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(5),
            Constraint::Length(8),
            Constraint::Length(6),
        ])
        .split(area);

    let header = Paragraph::new(format!(
        " ACP Dashboard  |  models: {}  |  pipelines: {}  |  [r] refresh  [q] quit",
        state.models.len(),
        state.pipelines.len(),
    ))
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(header, vertical[0]);

    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(vertical[1]);

    let pipeline_items: Vec<ListItem> = state
        .pipelines
        .iter()
        .take(20)
        .map(|p| {
            let color = match p.status {
                PipelineStatus::Succeeded => Color::Green,
                PipelineStatus::Failed => Color::Red,
                PipelineStatus::Running => Color::Yellow,
                _ => Color::White,
            };
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:.8}  ", p.id),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("{:<12}", p.status.to_string()),
                    Style::default().fg(color),
                ),
                Span::raw(p.profile.to_string()),
            ]))
        })
        .collect();
    let pipeline_list =
        List::new(pipeline_items).block(Block::default().title("Pipelines").borders(Borders::ALL));
    frame.render_widget(pipeline_list, horizontal[0]);

    let model_items: Vec<ListItem> = state
        .models
        .iter()
        .take(20)
        .map(|m| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:<8}", m.tier.to_string()),
                    Style::default().fg(Color::Cyan),
                ),
                Span::raw(format!("  {}", m.name)),
            ]))
        })
        .collect();
    let model_list =
        List::new(model_items).block(Block::default().title("Models").borders(Borders::ALL));
    frame.render_widget(model_list, horizontal[1]);

    let recent: Vec<ListItem> = state
        .events
        .iter()
        .rev()
        .take(3)
        .map(|e| ListItem::new(Line::from(Span::raw(e.as_str()))))
        .collect();
    let log = List::new(recent)
        .block(
            Block::default()
                .title("Recent events")
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::Gray));
    frame.render_widget(log, vertical[2]);

    let advanced = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(34),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(vertical[3]);

    let scheduler_items: Vec<ListItem> = state
        .scheduler
        .iter()
        .rev()
        .take(5)
        .map(|d| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:<12}", truncate(&d.role, 11)),
                    Style::default().fg(Color::White),
                ),
                Span::raw(format!(" {} {:.2}", d.runtime_type, d.final_score)),
            ]))
        })
        .collect();
    frame.render_widget(
        List::new(scheduler_items).block(Block::default().title("Scheduler").borders(Borders::ALL)),
        advanced[0],
    );

    let memory_items: Vec<ListItem> = state
        .compressions
        .iter()
        .rev()
        .take(3)
        .map(|c| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:<12}", truncate(&c.role, 11)),
                    Style::default().fg(Color::Cyan),
                ),
                Span::raw(format!(
                    " {} refs:{}",
                    truncate(&c.compressor, 16),
                    c.semantic_refs.len()
                )),
            ]))
        })
        .chain(state.semantic_memory.iter().rev().take(2).map(|m| {
            ListItem::new(Line::from(vec![
                Span::styled("mem ", Style::default().fg(Color::DarkGray)),
                Span::raw(truncate(&m.item_id, 28).to_string()),
            ]))
        }))
        .collect();
    frame.render_widget(
        List::new(memory_items).block(Block::default().title("Memory").borders(Borders::ALL)),
        advanced[1],
    );

    let mcp_items: Vec<ListItem> = state
        .mcp_health
        .iter()
        .take(5)
        .map(|h| {
            let color = if h.status == RuntimeHealth::Healthy {
                Color::Green
            } else {
                Color::Yellow
            };
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:<14}", truncate(&h.name, 13)),
                    Style::default().fg(Color::White),
                ),
                Span::styled(h.status.to_string(), Style::default().fg(color)),
            ]))
        })
        .collect();
    frame.render_widget(
        List::new(mcp_items).block(Block::default().title("MCP").borders(Borders::ALL)),
        advanced[2],
    );

    // Analytics panel: last N step metrics
    let metric_items: Vec<ListItem> = state
        .metrics
        .iter()
        .rev()
        .take(4)
        .map(|m| {
            let health_color = if m.health == RuntimeHealth::Healthy {
                Color::Green
            } else {
                Color::Red
            };
            let latency = m
                .latency_ms
                .map(|v| format!("{v}ms"))
                .unwrap_or_else(|| "–".to_string());
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:<22}", truncate(&m.step_name, 21)),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("{:<10}", m.health.to_string()),
                    Style::default().fg(health_color),
                ),
                Span::styled(latency, Style::default().fg(Color::DarkGray)),
            ]))
        })
        .collect();
    let metrics_list = List::new(metric_items)
        .block(
            Block::default()
                .title("Step analytics")
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::Gray));
    frame.render_widget(metrics_list, vertical[4]);
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        s
    } else {
        &s[..max]
    }
}
