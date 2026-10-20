#!/usr/bin/env python3
"""Generate host files from the host-neutral recipes.

    recipes.py generate   # write the generated files below from recipes/<name>.md
    recipes.py check      # exit 1 if any generated file differs from disk

Two outputs per recipe:
- plugin/commands/<name>.md: a Claude Code slash command.
- plugin/skills/ai-crew-sync/references/<name>.md: the same procedure as a
  reference of the ai-crew-sync skill, for hosts without slash commands
  (Google Antigravity loads skills, not commands), listed in
  references/index.md.

A recipe's first line is `<!-- slash: description="…" hint="…" -->`; the
body follows, with `{{input}}` where the command receives $ARGUMENTS.
"""
import glob, os, re, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RECIPES = os.path.join(ROOT, "recipes")
COMMANDS = os.path.join(ROOT, "plugin", "commands")
REFERENCES = os.path.join(ROOT, "plugin", "skills", "ai-crew-sync", "references")
HEAD = re.compile(r'<!-- slash: description="([^"]*)" hint="([^"]*)" -->\n')


def render(recipe_path):
    text = open(recipe_path).read()
    m = HEAD.match(text)
    if not m:
        sys.exit(f"{recipe_path}: first line must be <!-- slash: description=\"…\" hint=\"…\" -->")
    body = text[m.end():].replace("{{input}}", "$ARGUMENTS")
    return f'---\ndescription: {m.group(1)}\nargument-hint: "{m.group(2)}"\n---\n\n{body}'


def render_reference(recipe_path):
    text = open(recipe_path).read()
    m = HEAD.match(text)
    if not m:
        sys.exit(f"{recipe_path}: first line must be <!-- slash: description=\"…\" hint=\"…\" -->")
    name = os.path.basename(recipe_path)[:-3]
    # `Input: {{input}}` introduces what the user asked; anywhere else the
    # placeholder stands for a value inside a sentence ("the last {{input}}
    # hours"), where a whole phrase would garble it.
    body = text[m.end():].replace("Input: {{input}}", "Input: the user's request, in their words")
    body = body.replace("{{input}}", "<the user's arguments>")
    return (
        f"<!-- Generated from recipes/{name}.md by `make recipes`; edit the recipe. -->\n"
        f"# {name}: {m.group(1)}\n\n"
        f"Arguments, when the user gives any: {m.group(2)}\n\n{body}"
    )


def render_index(recipes):
    lines = [
        "<!-- Generated from recipes/ by `make recipes`; edit the recipes. -->",
        "# ai-crew-sync procedures",
        "",
        "One file per procedure. Read the one that matches what the user asked for,",
        "then follow its steps.",
        "",
    ]
    for recipe in recipes:
        name = os.path.basename(recipe)[:-3]
        m = HEAD.match(open(recipe).read())
        lines.append(f"- [{name}]({name}.md): {m.group(1) if m else ''}")
    return "\n".join(lines) + "\n"


def main(mode):
    recipes = [
        r for r in sorted(glob.glob(os.path.join(RECIPES, "*.md")))
        if os.path.basename(r) != "README.md"
    ]
    wanted = {}
    for recipe in recipes:
        name = os.path.basename(recipe)[:-3]
        wanted[os.path.join(COMMANDS, f"{name}.md")] = render(recipe)
        wanted[os.path.join(REFERENCES, f"{name}.md")] = render_reference(recipe)
    wanted[os.path.join(REFERENCES, "index.md")] = render_index(recipes)
    if mode == "generate":
        os.makedirs(REFERENCES, exist_ok=True)
        for path, text in wanted.items():
            open(path, "w").write(text)
        # A renamed or deleted recipe leaves its generated files behind; the
        # check names them as strays, so generating removes them.
        for d in (COMMANDS, REFERENCES):
            for p in glob.glob(os.path.join(d, "*.md")):
                if p not in wanted:
                    os.remove(p)
        return
    drift = [
        os.path.relpath(path, ROOT)
        for path, text in wanted.items()
        if not os.path.exists(path) or open(path).read() != text
    ]
    strays = sorted(
        os.path.relpath(p, ROOT)
        for d in (COMMANDS, REFERENCES)
        for p in glob.glob(os.path.join(d, "*.md"))
        if p not in wanted
    )
    if drift or strays:
        for n in drift:
            print(f"{n} differs from its recipe")
        for n in strays:
            print(f"{n} has no recipe")
        sys.exit("run `make recipes`: the recipes are the only place a procedure is edited")
    print("recipes: every slash command and skill reference matches its recipe")


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("generate", "check"):
        sys.exit(__doc__)
    main(sys.argv[1])
