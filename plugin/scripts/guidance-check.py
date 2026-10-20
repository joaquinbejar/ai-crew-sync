#!/usr/bin/env python3
"""Fail when the guidance we ship disagrees with the code it describes (#225).

    guidance-check.py     # exit 1 and say what is wrong, or print one line

Three checks, all offline:

- The plugin manifest's version is the crate's: a release that bumps one and
  not the other ships a plugin that describes a different binary.
- No bundled guidance repeats a claim that stopped being true. The list is
  the claims retired when idle windows became wakeable (Claude Code watcher,
  Codex adapter); add to it whenever a statement is retired, so it cannot
  come back by copy and paste.
- Every wake mechanism the code announces (`attention.wake` labels) is named
  in the skill, both READMEs and the host acceptance document, so a reader of
  any of them can recognise what `list_sessions` shows.
"""
import glob, json, os, re, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RETIRED = [
    r"have no way to be woken",
    r"Nothing wakes a model",
    r"This does not make an idle session",
    r"Esto no hace contestable",
]

def files(pattern):
    """Repository-relative paths matching `pattern` (Python 3.9 has no root_dir)."""
    return [
        os.path.relpath(p, ROOT)
        for p in glob.glob(os.path.join(ROOT, pattern), recursive=True)
    ]


GUIDANCE = (
    ["README.md", "README.es.md"]
    + files("plugin/skills/**/*.md")
    + files("plugin/commands/*.md")
    + files("recipes/*.md")
    + files("docs/**/*.md")
    + files("examples/*")
)

NAMES_WAKE = [
    "plugin/skills/ai-crew-sync/SKILL.md",
    "README.md",
    "README.es.md",
    "docs/acceptance/host-integration.md",
]


def read(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as fh:
        return fh.read()


def main():
    problems = []

    crate = re.search(r'(?m)^version\s*=\s*"([^"]+)"', read("Cargo.toml"))
    plugin = json.loads(read("plugin/.claude-plugin/plugin.json")).get("version")
    if not crate or plugin != crate.group(1):
        problems.append(
            f"plugin/.claude-plugin/plugin.json version {plugin!r} differs from "
            f"Cargo.toml {crate.group(1) if crate else None!r}"
        )

    for path in sorted(set(GUIDANCE)):
        if not os.path.isfile(os.path.join(ROOT, path)):
            continue
        text = read(path)
        for claim in RETIRED:
            if re.search(claim, text):
                problems.append(f"{path} repeats a retired claim: /{claim}/")

    labels = set()
    for source in files("src/**/*.rs"):
        labels.update(re.findall(r'WAKE_LABEL: &str = "([^"]+)"', read(source)))
    if not labels:
        problems.append("no wake label constant found in src/ (WAKE_LABEL)")
    for path in NAMES_WAKE:
        text = read(path)
        for label in sorted(labels):
            if label not in text:
                problems.append(f"{path} never names the wake mechanism '{label}'")

    if problems:
        for p in problems:
            print(p)
        sys.exit("guidance: fix the files above, or retire the claim deliberately")
    print(f"guidance: version {plugin}, no retired claims, wake labels {sorted(labels)} named")


if __name__ == "__main__":
    main()
