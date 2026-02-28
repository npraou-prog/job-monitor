# Job Monitor 🔍

Monitor company career pages for new job postings. Get alerted when roles matching your criteria appear.

## How It Works

1. **Configure** companies & referrers in `config.json`
2. **Run** the scanner (manually triggered)
3. **Check** `run-log.md` for results
4. **Act** — reach out to referrers when relevant jobs appear

## Files

| File | Purpose |
|------|---------|
| `config.json` | Companies, URLs, referrers, target roles |
| `jobs-db.json` | All known jobs (baseline + history) |
| `run-log.md` | Run history table |
| `scanner.py` | The scanner script |

## Usage

```bash
# Run scan
python scanner.py

# Or via OpenClaw
"check jobs" / "run job monitor"
```

## Target Roles

Configured in `config.json`:
- Data Scientist
- Data Analyst
- Machine Learning
- ML Engineer
- AI Engineer
- AIML

## Adding Companies

Edit `config.json`:

```json
{
  "companies": [
    {
      "id": "company-slug",
      "name": "Company Name",
      "url": "https://careers.company.com/jobs",
      "referrers": ["Person1", "Person2"],
      "added": "2026-02-27"
    }
  ]
}
```

---

*Built for RefHunt 2026* 🎯
