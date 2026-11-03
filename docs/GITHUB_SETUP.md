# Publish your first GitHub project (beginner-friendly)

## Create your own account

1. Visit https://github.com/signup and register with an email address you control. Pick a simple professional username; this becomes part of your public GitHub URL.
2. Verify the address and configure two-factor authentication. Do not share passwords or one-time codes.
3. Install **Git** from https://git-scm.com/downloads and install Python 3.10+ from https://www.python.org/downloads/ if needed.
4. Download this project ZIP and **extract** it; do not upload the ZIP itself as your entire repo.
5. On https://github.com/new create a public repository named `real-time-ai-event-pipeline`. **Do not initialize it** with a README, .gitignore, or license because the project already has these files.

## First upload with Git

Open VS Code terminal, PowerShell or Git Bash inside the extracted `real-time-ai-event-pipeline` folder:

```bash
git config --global user.name "Your Name"
git config --global user.email "your-email@example.com"
git init
git branch -M main
git add .
git commit -m "Build Kafka, AI, Lambda, Snowflake and dbt event pipeline"
git remote add origin https://github.com/YOUR_USERNAME/real-time-ai-event-pipeline.git
git push -u origin main
```

Replace `Your Name`, the email, and `YOUR_USERNAME`. GitHub no longer accepts an account password to authenticate Git operations over HTTPS: when prompted, use your OS credential manager / Git Credential Manager, or follow GitHub's token/SSH setup. Never add tokens to the repository.

## Easier alternative: browser upload

If you don't want to use Git commands yet: open the new empty repo and select **uploading an existing file**, drag the extracted project files and folders, and commit. GitHub's browser upload has practical file-count/size limits; Git is recommended.

## After publishing

- Open the **Actions** tab and verify **Python CI** runs successfully.
- Pin the repo on your GitHub profile (profile > Customize your pins).
- Add the link in your resume and LinkedIn Featured section.
- Always keep `.env`, model artifacts, and database exports out of your commits; `.gitignore` already excludes them.
- To publish future changes: `git add . && git commit -m "Describe update" && git push`.
