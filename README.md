# Campaign Template Markdown Generator

Fetches `core_campaign_template.sql` and lookup tables from the private `cognitiv/ML-research-science` GitHub repo, parses all active INSERT statements, resolves IDs to human-readable names, and outputs a formatted Markdown table to `campaign_templates_documentation.md`.

---

## Setup

### 1. Create a GitHub Personal Access Token (PAT)

1. Go to [GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)](https://github.com/settings/tokens)
2. Click **Generate new token (classic)**
3. Give it a name (e.g. `campaign-template-gen`)
4. Under **Scopes**, check `repo` (full repo access for private repos)
5. Click **Generate token** and copy the token

### 2. Add token to `.env`

Create a `.env` file in the project root:

```
GITHUB_TOKEN=your_token_here
```

> The `.env` file is gitignored and will not be committed.

---

## Usage

```bash
python3 core_campaign_template_markdown_generator.py <branch>
```

**Example:**

```bash
python3 core_campaign_template_markdown_generator.py dev/enum_pull
```

This generates `campaign_templates_documentation.md` in the current directory.

---

## Requirements

- Python 3.7+
- No third-party dependencies (uses stdlib only)
