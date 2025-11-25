// Minimal Tauri wrapper that launches the existing FastAPI UI server
// and opens it in a desktop window.
#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use std::{
    env,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tauri::{Manager, WindowEvent};

#[derive(Clone)]
struct ServerState(Arc<Mutex<Option<Child>>>);

impl ServerState {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    fn start(&self) -> Result<(), String> {
        let mut guard = self.0.lock().map_err(|e| e.to_string())?;
        if guard.is_some() {
            return Ok(());
        }
        let port = env::var("HBD_UI_PORT").unwrap_or_else(|_| "8000".to_string());
        // Prefer bundled venv python, allow override; bootstrap if missing.
        let mut python = env::var("HBD_PYTHON").ok().filter(|s| !s.is_empty());
        if python.is_none() {
            python = bundled_python_path();
        }
        if python.is_none() {
            bootstrap_venv();
            python = bundled_python_path();
        }
        let python = python.unwrap_or_else(|| "python".to_string());

        // Find repo root (contains humblebundle_downloader) by walking up from cwd.
        let mut workdir = env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        if let Ok(custom) = env::var("HBD_WORKDIR") {
            workdir = std::path::PathBuf::from(custom);
        } else {
            workdir = find_repo_root(&workdir).unwrap_or(workdir);
        }
        let py_path = workdir.to_string_lossy().to_string();
        let merged_pythonpath = match env::var("PYTHONPATH") {
            Ok(existing) if !existing.is_empty() => format!("{py_path}{}{}", std::path::MAIN_SEPARATOR, existing),
            _ => py_path.clone(),
        };

        let mut cmd = Command::new(python);
        cmd.args(["-m", "humblebundle_downloader.ui_server"])
            .current_dir(&workdir)
            .env("PORT", &port)
            .env("PYTHONUNBUFFERED", "1")
            // Ensure the module is importable.
            .env("PYTHONPATH", merged_pythonpath)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        let child = cmd.spawn().map_err(|e| format!("Failed to start UI server: {e}"))?;
        *guard = Some(child);
        Ok(())
    }

    fn stop(&self) {
        if let Ok(mut guard) = self.0.lock() {
            if let Some(child) = guard.as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
            *guard = None;
        }
    }
}

fn main() {
    tauri::Builder::default()
        .manage(ServerState::new())
        .setup(|app| {
            if bundled_python_path().is_none() {
                bootstrap_venv();
            }
            let state = app.state::<ServerState>().clone();
            state.start()?;
            // Give the server a brief head start before the window loads the URL.
            std::thread::sleep(Duration::from_millis(300));
            Ok(())
        })
        .on_window_event(|event| {
            if let WindowEvent::CloseRequested { .. } = event.event() {
                let state = event.window().state::<ServerState>().clone();
                state.stop();
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running Tauri application");
}

fn find_repo_root(start: &std::path::Path) -> Option<std::path::PathBuf> {
    let mut current = start.to_path_buf();
    for _ in 0..6 {
        if current.join("humblebundle_downloader").is_dir() && current.join("pyproject.toml").exists() {
            return Some(current);
        }
        if !current.pop() {
            break;
        }
    }
    None
}

fn locate_project_root() -> Option<std::path::PathBuf> {
    // Try current dir first, then executable dir.
    if let Some(root) = find_repo_root(&std::env::current_dir().unwrap_or_default()) {
        return Some(root);
    }
    if let Ok(mut exe_dir) = std::env::current_exe() {
        exe_dir.pop();
        if let Some(root) = find_repo_root(&exe_dir) {
            return Some(root);
        }
    }
    None
}

fn bundled_python_path() -> Option<String> {
    if let Some(root) = locate_project_root() {
        let win = root.join("desktop").join("tauri-app").join(".venv").join("Scripts").join("python.exe");
        if win.exists() {
            return Some(win.to_string_lossy().to_string());
        }
        let unix = root.join("desktop").join("tauri-app").join(".venv").join("bin").join("python");
        if unix.exists() {
            return Some(unix.to_string_lossy().to_string());
        }
    }
    None
}

fn bootstrap_venv() {
    let base = locate_project_root().unwrap_or_else(|| std::path::PathBuf::from("."));
    let ps1 = base.join("desktop").join("tauri-app").join("bootstrap.ps1");
    let sh = base.join("desktop").join("tauri-app").join("bootstrap.sh");
    if ps1.exists() {
        let _ = Command::new("powershell")
            .args(["-ExecutionPolicy", "Bypass", "-File", &ps1.to_string_lossy()])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status();
    } else if sh.exists() {
        let _ = Command::new("bash")
            .arg(sh.to_string_lossy().to_string())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status();
    }
}
