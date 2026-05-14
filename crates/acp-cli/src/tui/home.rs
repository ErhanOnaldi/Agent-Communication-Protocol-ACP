use std::{io, time::Duration};

use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap},
    Frame, Terminal,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HomeAction {
    Doctor,
    Discover,
    Models,
    Dashboard,
    Quit,
}

pub async fn run_home() -> anyhow::Result<HomeAction> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut selected = HomeAction::Dashboard;
    loop {
        terminal.draw(|frame| draw_home(frame, selected))?;

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => {
                        selected = HomeAction::Quit;
                        break;
                    }
                    KeyCode::Enter => break,
                    KeyCode::Char('d') => {
                        selected = HomeAction::Doctor;
                        break;
                    }
                    KeyCode::Char('r') => {
                        selected = HomeAction::Discover;
                        break;
                    }
                    KeyCode::Char('m') => {
                        selected = HomeAction::Models;
                        break;
                    }
                    KeyCode::Char('b') => {
                        selected = HomeAction::Dashboard;
                        break;
                    }
                    KeyCode::Up | KeyCode::Char('k') => selected = previous(selected),
                    KeyCode::Down | KeyCode::Char('j') => selected = next(selected),
                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(selected)
}

fn draw_home(frame: &mut Frame, selected: HomeAction) {
    let area = frame.area();
    frame.render_widget(Clear, area);

    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(5),
            Constraint::Length(2),
            Constraint::Length(7),
            Constraint::Length(4),
            Constraint::Min(1),
        ])
        .split(area);

    let logo = Paragraph::new(Text::from(vec![
        Line::from("    ___       ______     ____  "),
        Line::from("   /   |     / ____/    / __ \\ "),
        Line::from("  / /| |    / /        / /_/ / "),
        Line::from(" / ___ |   / /___     / ____/  "),
        Line::from("/_/  |_|   \\____/    /_/       "),
    ]))
    .alignment(Alignment::Center)
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );
    frame.render_widget(logo, outer[1]);

    let subtitle = Paragraph::new("AI Control Plane")
        .alignment(Alignment::Center)
        .style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(subtitle, outer[2]);

    let menu = centered_rect(70, 100, outer[3]);
    let items = [
        menu_item(
            selected,
            HomeAction::Dashboard,
            "b / Enter",
            "Open dashboard",
        ),
        menu_item(selected, HomeAction::Doctor, "d", "Run doctor"),
        menu_item(selected, HomeAction::Discover, "r", "Discover runtimes"),
        menu_item(selected, HomeAction::Models, "m", "List models"),
        menu_item(selected, HomeAction::Quit, "q / Esc", "Quit"),
    ];
    frame.render_widget(
        List::new(items).block(
            Block::default()
                .title("Choose an action")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        ),
        menu,
    );

    let help = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("Tip: ", Style::default().fg(Color::Cyan)),
            Span::raw("subcommands still work: "),
            Span::styled("acp doctor", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(", "),
            Span::styled(
                "acp pipeline run workflow.yaml --execute",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(", "),
            Span::styled(
                "acp dashboard",
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from("Use Up/Down or j/k, Enter to run the selected action."),
    ])
    .alignment(Alignment::Center)
    .wrap(Wrap { trim: true })
    .style(Style::default().fg(Color::Gray));
    frame.render_widget(help, outer[4]);
}

fn menu_item<'a>(
    selected: HomeAction,
    action: HomeAction,
    key: &'a str,
    label: &'a str,
) -> ListItem<'a> {
    let is_selected = selected == action;
    let marker = if is_selected { ">" } else { " " };
    let style = if is_selected {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White)
    };
    ListItem::new(Line::from(vec![
        Span::raw(format!(" {marker} ")),
        Span::styled(format!("{key:<10}"), Style::default().fg(Color::Yellow)),
        Span::raw(label),
    ]))
    .style(style)
}

fn next(current: HomeAction) -> HomeAction {
    match current {
        HomeAction::Dashboard => HomeAction::Doctor,
        HomeAction::Doctor => HomeAction::Discover,
        HomeAction::Discover => HomeAction::Models,
        HomeAction::Models => HomeAction::Quit,
        HomeAction::Quit => HomeAction::Dashboard,
    }
}

fn previous(current: HomeAction) -> HomeAction {
    match current {
        HomeAction::Dashboard => HomeAction::Quit,
        HomeAction::Doctor => HomeAction::Dashboard,
        HomeAction::Discover => HomeAction::Doctor,
        HomeAction::Models => HomeAction::Discover,
        HomeAction::Quit => HomeAction::Models,
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
