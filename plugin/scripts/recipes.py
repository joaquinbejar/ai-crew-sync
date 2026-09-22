#!/usr/bin/env python3
"""Generate the Claude Code slash commands from the host-neutral recipes.

    recipes.py generate   # write plugin/commands/<name>.md from recipes/<name>.md
    recipes.py check      # exit 1 if any generated command differs from disk

A recipe's first line is `<!-- slash: description="…" hint="…" -->`; the
body follows, with `{{input}}` where the command receives $ARGUMENTS.
"""
import glob, os, re, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RECIPES = os.path.join(ROOT, "recipes")
COMMANDS = os.path.join(ROOT, "plugin", "commands")
HEAD = re.compile(r'<!-- slash: description="([^"]*)" hint="([^"]*)" -->\n')


def render(recipe_path):
    text = open(recipe_path).read()
    m = HEAD.match(text)
    if not m:
        sys.exit(f"{recipe_path}: first line must be <!-- slash: description=\"…\" hint=\"…\" -->")
    body = text[m.end():].replace("{{input}}", "$ARGUMENTS")
    return f'---\ndescription: {m.group(1)}\nargument-hint: "{m.group(2)}"\n---\n\n{body}'


def main(mode):
    drift = []
    for recipe in sorted(glob.glob(os.path.join(RECIPES, "*.md"))):
        name = os.path.basename(recipe)[:-3]
        if name == "README":
            continue
        target = os.path.join(COMMANDS, f"{name}.md")
        wanted = render(recipe)
        if mode == "generate":
            open(target, "w").write(wanted)
        elif not os.path.exists(target) or open(target).read() != wanted:
            drift.append(name)
    if mode == "check":
        strays = sorted(
            os.path.basename(p)[:-3]
            for p in glob.glob(os.path.join(COMMANDS, "*.md"))
            if not os.path.exists(os.path.join(RECIPES, os.path.basename(p)))
        )
        if drift or strays:
            for n in drift:
                print(f"plugin/commands/{n}.md differs from recipes/{n}.md")
            for n in strays:
                print(f"plugin/commands/{n}.md has no recipe")
            sys.exit("run `make recipes`: the recipes are the only place a procedure is edited")
        print("recipes: every slash command matches its recipe")


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("generate", "check"):
        sys.exit(__doc__)
    main(sys.argv[1])
