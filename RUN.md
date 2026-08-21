# How to run this project

Every command below is meant to be pasted into the **VS Code integrated terminal**
(`` Ctrl+` `` to open it). Windows PowerShell is the default on Windows; the
macOS/Linux equivalent is given wherever it differs.

> **Prefer not to install anything?** The portfolio site has a **Run this project**
> button that opens this repository in a free GitHub Codespace, installs the
> dependencies and runs the whole pipeline for you:
> <https://nikhil201716.github.io/nikhil-data-portfolio/pages/project.html?id=06>

---

## 1. Prerequisites

```powershell
python --version    # 3.11 or newer
git --version
```

### Required — Linux (WSL on Windows)

This project needs a Linux environment. On Windows use WSL, or skip the setup entirely and click **Run this project** on the portfolio site, which opens a Codespace where it already works.

```powershell
wsl --install          # once, then reopen VS Code and use the WSL terminal
```

### Optional — the local LLM stages

This project has stages that use a local model through [Ollama](https://ollama.com). They are **optional**: without it those stages are skipped or fall back to their deterministic control arm, and every other stage runs normally.

```powershell
winget install Ollama.Ollama
ollama pull qwen2.5:0.5b
ollama list
```

---

---

## 2. One-time setup

```powershell
git clone https://github.com/Nikhil201716/06-Realtime-Transaction-Streaming-Pipeline.git
cd 06-Realtime-Transaction-Streaming-Pipeline
```

Create and activate a virtual environment. This keeps the project's dependencies
from colliding with anything else on your machine.

**Windows (PowerShell)**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

<details>
<summary>If PowerShell refuses to run the activation script</summary>

Windows blocks unsigned scripts by default. Allow them for your own user account only:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```
</details>

**macOS / Linux**

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Then install the dependencies:

```powershell
pip install -r requirements.txt
```

> **Tip:** with the venv active, VS Code will offer to select it as the interpreter.
> Accept — otherwise the Run and Debug buttons use your global Python and you get
> `ModuleNotFoundError`.

---

## 3. Run it

This project does not have a single-command runner — it needs services running alongside it. Run these in order:

**1. Start Redis**

```bash
redis-server --daemonize yes
redis-cli ping
```

**2. Build the account master**

```bash
python scripts/generate_reference_data.py
```

**3. Stream transactions (leave running)**

```bash
python scripts/producer.py --duration 180 --rate 2.5
```

**4. Score the stream in a second terminal**

```bash
python pipeline/consumer.py --duration 200
```

**5. Grade the detector against the injected labels**

```bash
python scripts/evaluate_detection.py
```

---

## 4. Explore the results

```powershell
streamlit run dashboard/streamlit_app.py
```

Opens on <http://localhost:8501>. VS Code will offer to forward the port and open it in your browser.

The pipeline writes everything it measures into `reports/`. Those files are the
source of every number quoted on the portfolio site — nothing is typed by hand.

```powershell
ls reports
```

---

## 5. What a correct run looks like

Expect precision near 97.2% and recall near 79.5%, with all nine misses being card-testing events. That recall is the detector's structural ceiling, not a bug.

---

## 6. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError` | the virtual environment is not active | re-run the activate command from step 2 |
| `FileNotFoundError` on a data file | an earlier stage was skipped | run the stages in the documented order, or use the one-command runner |
| `Activate.ps1 cannot be loaded` | PowerShell execution policy | `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned` |
| Numbers differ from the README | a seed or parameter changed | check the constants at the top of the generator script |
| `command not found` | dependency missing from this environment | `pip install -r requirements.txt` with the venv active |
| VS Code runs the wrong Python | interpreter not selected | `Ctrl+Shift+P` → *Python: Select Interpreter* → pick `.venv` |

---

## 7. Finish

```powershell
deactivate
```

---

## More

- **The 60+ page technical notebook** for this project is in [`docs/`](docs/) — it
  covers the business problem, the mathematics derived from first principles, a
  guided tour of the code, worked numerical examples and exercises with solutions.
- **All fifteen projects:** <https://nikhil201716.github.io/nikhil-data-portfolio/>

*Generated from this repository's own pipeline runner, so the stage list cannot
drift from the code.*
