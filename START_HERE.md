# Your first 5 steps

**1. Download and extract** the project ZIP to a folder on your computer.

**2. Install Python and Git** if necessary. Open the project folder in VS Code.

**3. Run the demo** from VS Code Terminal (Windows PowerShell):

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e '.[dev]'
.\.venv\Scripts\python.exe -m event_pipeline.cli demo --count 150
.\.venv\Scripts\python.exe -m pytest -q
```

A generated SQLite file and alerts CSV appear in `data/`. There is no cloud bill for this demo.

**4. Create your GitHub account** at https://github.com/signup and verify your email. Set up two-factor authentication. Then open https://github.com/new and create a **public** repo named `real-time-ai-event-pipeline`, without adding a starter README. See [`docs/GITHUB_SETUP.md`](docs/GITHUB_SETUP.md).

**5. Push this project** (replace your email/name/username):

```powershell
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
git init
git branch -M main
git add .
git commit -m "Initial commit: Kafka + AI event pipeline"
git remote add origin https://github.com/YOUR_USERNAME/real-time-ai-event-pipeline.git
git push -u origin main
```

Once published, the repository can be pinned to your GitHub profile and linked on your resume. You don't need AWS, Snowflake or OpenAI API credentials to run the local demo, but the cloud and optional LLM features require additional setup.
