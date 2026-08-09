# Getting a real Redis server running (WSL, compiled from source)

This project needs an actual Redis server - not a mock, not `fakeredis` -
because the whole point is exercising real Redis Streams consumer-group
semantics (`XREADGROUP`/`XACK`) and real Sorted Set velocity tracking.
Here's exactly how it was set up on this machine, including the dead end
that came first, so the reasoning is honest and reproducible.

## What was tried first: Memurai (native Windows, no WSL)

[Memurai](https://www.memurai.com/) is a Redis-compatible server built
for Windows - the RAM-friendlier choice for a machine with a tight budget,
since it avoids running a second OS layer. It was installed via:

```powershell
winget install Memurai.MemuraiDeveloper
```

This downloaded and signature-verified correctly but the installer failed
twice - first with "another installation already in progress" (exit
1618), then with exit 1603 on retry. The real cause was found in the MSI
log at:

```
%LocalAppData%\Packages\Microsoft.DesktopAppInstaller_8wekyb3d8bbwe\LocalState\DiagOutputDir\Memurai.MemuraiDeveloper.*.log
```

```
SFXCA: Failed to create temp directory. Error code 5
```

Error code 5 is Windows for "Access Denied" - a permissions issue in the
installer's custom action (`ca_SilentCheckIfPortIsAvailable`), unrelated
to disk space, RAM, or anything this project's code controls. Chasing a
Windows Installer permissions bug would mean poking at system-level
settings, which this project deliberately avoids. So: pivot to WSL, which
was already set up for [Project 4](../../04-Multi-Source-Sales-ETL-Pipeline-Airflow-AWS/)'s
Airflow environment.

## What actually worked: compiling Redis from official source in WSL

No `apt`/`sudo` package install was needed - `gcc` and `make` were
already present in the WSL Ubuntu image from Project 4's setup, and
Redis's build system has zero required external dependencies beyond a C
compiler.

```bash
wsl -d Ubuntu -- bash -c "
  cd ~ &&
  curl -O https://download.redis.io/redis-stable.tar.gz &&
  tar xzf redis-stable.tar.gz &&
  cd redis-stable &&
  make redis-server
"
```

Two real complications came up on this specific machine, both worth
recording:

**1. Backgrounding inside a one-shot `wsl -d Ubuntu -- bash -c "..."`
call doesn't reliably persist.** A plain `nohup make redis-server &`
inside that invocation gets killed when the outer `wsl.exe` process
exits, because it isn't truly detached from the parent's process group.
Fix - use `setsid` (starts a new session, detaching from the controlling
terminal entirely) plus explicit `disown`:

```bash
wsl -d Ubuntu -- bash -c "cd ~/redis-stable && setsid nohup make redis-server > /tmp/redis_build.log 2>&1 < /dev/null & disown; echo started"
```

**2. The build is slow and CPU/RAM-heavy on a constrained machine** -
Redis's default build uses `-flto=auto` (link-time optimization), which
compiles every `.o` file individually and then re-optimizes and links
them together in a second, much heavier pass (visible as multiple
parallel `lto1`/`cc1` processes in `ps aux` near the end). On a healthy
machine this whole build takes 1-3 minutes; under this machine's RAM/CPU
contention it took closer to 30-40 minutes. It never failed, it was just
slow - patience (and polling `ps aux | grep -E 'cc1|lto1|make'` plus
`tail`-ing the build log) was the only thing needed.

## Running the server

```bash
wsl -d Ubuntu -- bash -c "cd ~/redis-stable/src && setsid nohup ./redis-server --port 6379 --logfile /tmp/redis_server.log > /tmp/redis_stdout.log 2>&1 < /dev/null & disown"
```

WSL2 forwards `localhost:<port>` between Windows and the Linux VM
automatically, so `redis-py` running in a normal Windows Python process
connects with zero extra configuration:

```python
import redis
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
r.ping()  # True
```

## Verifying it's real

```bash
wsl -d Ubuntu -- bash -c "~/redis-stable/src/redis-server --version"
# Redis server v=8.10.0 sha=00000000:1 malloc=jemalloc-5.3.0 bits=64 build=...

wsl -d Ubuntu -- bash -c "~/redis-stable/src/redis-cli ping"
# PONG
```

## For anyone else running this project

If you already have Redis (via Docker, WSL, a package manager, or a
native Windows build like Memurai working correctly on your machine),
just point `pipeline/redis_client.py` at it - it reads `REDIS_HOST` /
`REDIS_PORT` environment variables (defaults: `localhost:6379`). None of
the pipeline code is WSL-specific; only the *installation method* on this
particular machine was.
