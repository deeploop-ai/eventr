# EventR Skills

Agent skills for [EventR](https://github.com/deeploop-ai/eventr), published for the [skills.sh](https://skills.sh/) ecosystem.

Install with the [Skills CLI](https://github.com/vercel-labs/skills):

```bash
# Install into the current project (Cursor, Claude Code, Codex, etc.)
npx skills add deeploop-ai/eventr@eventr

# Install globally for all projects
npx skills add deeploop-ai/eventr@eventr -g -y

# Try without installing
npx skills use deeploop-ai/eventr@eventr
```

After publishing to GitHub, the skill page will appear at:

`https://skills.sh/deeploop-ai/eventr/eventr`

## Skills in this directory

| Skill | Description |
|-------|-------------|
| [eventr](eventr/SKILL.md) | Author, validate, test, and run EventR DAG pipelines (YAML/HOCON, eql, plugins) |

## Layout

This repo follows the [Agent Skills](https://github.com/vercel-labs/skills) convention:

```
skills/
├── README.md           # this file
└── eventr/
    ├── SKILL.md        # required — name must match directory
    └── reference.md    # plugin & config reference (bundled with skill)
```

The CLI discovers skills under `skills/<name>/SKILL.md` automatically.

## Development

When editing skills locally:

```bash
# Install from this repo (path to repo root or skills dir)
npx skills add ./ --skill eventr

# Validate frontmatter: name matches directory, description present
npx skills init --help
```

See [docs/ai-agent.md](../docs/ai-agent.md) for the Agent integration roadmap.
