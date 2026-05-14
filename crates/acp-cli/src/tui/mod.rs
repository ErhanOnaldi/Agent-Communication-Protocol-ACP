pub mod draw;
pub mod state;

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_client::AgentClient;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};

use self::{draw::draw_dashboard, state::DashboardState};

pub async fn run_live_dashboard(hub_client: AgentClient) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let state = Arc::new(Mutex::new(DashboardState::default()));
    let state_bg = state.clone();

    let client_bg = hub_client.clone();
    let refresh_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let pipelines = client_bg.pipelines().await.unwrap_or_default();
            let models = client_bg.models().await.unwrap_or_default();
            let events: Vec<String> = if let Some(p) = pipelines.first() {
                client_bg
                    .pipeline_events(p.id)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .rev()
                    .take(10)
                    .map(|e| {
                        format!(
                            "{} — {}",
                            e.event_type,
                            e.agent_id.as_deref().unwrap_or("-")
                        )
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let mut s = state_bg.lock().unwrap();
            s.pipelines = pipelines;
            s.models = models;
            s.events = events;
            if s.quit {
                break;
            }
        }
    });

    loop {
        {
            let s = state.lock().unwrap();
            if s.quit {
                break;
            }
            let pipelines = s.pipelines.clone();
            let models = s.models.clone();
            let events = s.events.clone();
            drop(s);
            terminal.draw(|frame| draw_dashboard(frame, &pipelines, &models, &events))?;
        }

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => {
                        state.lock().unwrap().quit = true;
                        break;
                    }
                    KeyCode::Char('r') => {
                        if let Ok(pipelines) = hub_client.pipelines().await {
                            state.lock().unwrap().pipelines = pipelines;
                        }
                        if let Ok(models) = hub_client.models().await {
                            state.lock().unwrap().models = models;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    refresh_task.abort();
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}
