//! The team's procedures over the bus tools, as prose any MCP host can
//! follow. The files live in `recipes/`; the Claude Code plugin's slash
//! commands are generated from them (`plugin/scripts/recipes.py`), and this
//! module embeds them so `ai-crew-sync recipes <name>` prints one on a
//! machine that has the binary and not the repository.

/// One recipe: its file name without extension, and the file as written.
pub struct Recipe {
    pub name: &'static str,
    pub text: &'static str,
}

/// Every recipe, in name order. Adding a file to `recipes/` without adding
/// it here fails `every_recipe_file_is_embedded`.
pub const RECIPES: &[Recipe] = &[
    Recipe {
        name: "announce",
        text: include_str!("../recipes/announce.md"),
    },
    Recipe {
        name: "ask",
        text: include_str!("../recipes/ask.md"),
    },
    Recipe {
        name: "board",
        text: include_str!("../recipes/board.md"),
    },
    Recipe {
        name: "catchup",
        text: include_str!("../recipes/catchup.md"),
    },
    Recipe {
        name: "claim",
        text: include_str!("../recipes/claim.md"),
    },
    Recipe {
        name: "done",
        text: include_str!("../recipes/done.md"),
    },
    Recipe {
        name: "handoff",
        text: include_str!("../recipes/handoff.md"),
    },
    Recipe {
        name: "inbox",
        text: include_str!("../recipes/inbox.md"),
    },
    Recipe {
        name: "lock",
        text: include_str!("../recipes/lock.md"),
    },
    Recipe {
        name: "note",
        text: include_str!("../recipes/note.md"),
    },
    Recipe {
        name: "review",
        text: include_str!("../recipes/review.md"),
    },
    Recipe {
        name: "standup",
        text: include_str!("../recipes/standup.md"),
    },
    Recipe {
        name: "thread",
        text: include_str!("../recipes/thread.md"),
    },
    Recipe {
        name: "unlock",
        text: include_str!("../recipes/unlock.md"),
    },
    Recipe {
        name: "wait",
        text: include_str!("../recipes/wait.md"),
    },
    Recipe {
        name: "who",
        text: include_str!("../recipes/who.md"),
    },
];

/// The slash-command header a recipe opens with: `description` and the
/// argument hint. Other hosts skip the line as an HTML comment.
pub fn slash(text: &str) -> Option<(&str, &str)> {
    let line = text.lines().next()?;
    let rest = line.strip_prefix("<!-- slash: description=\"")?;
    let (description, rest) = rest.split_once("\" hint=\"")?;
    let (hint, _) = rest.split_once("\" -->")?;
    Some((description, hint))
}

/// The recipe without its header line: what an agent is told to do.
pub fn body(text: &str) -> &str {
    text.split_once('\n').map(|(_, rest)| rest).unwrap_or("")
}

/// The Claude Code command generated from a recipe, byte for byte what
/// `plugin/scripts/recipes.py generate` writes.
pub fn command(text: &str) -> Option<String> {
    let (description, hint) = slash(text)?;
    Some(format!(
        "---\ndescription: {description}\nargument-hint: \"{hint}\"\n---\n\n{}",
        body(text).replace("{{input}}", "$ARGUMENTS")
    ))
}

pub fn find(name: &str) -> Option<&'static Recipe> {
    RECIPES.iter().find(|r| r.name == name)
}

/// `ai-crew-sync recipes [name]`: the list with one line each, or one
/// recipe's body with `{{input}}` left for the host to fill.
pub fn print(name: Option<&str>) -> anyhow::Result<()> {
    match name {
        None => {
            for r in RECIPES {
                let description = slash(r.text).map(|(d, _)| d).unwrap_or("");
                println!("{:<10} {description}", r.name);
            }
            println!(
                "\nPrint one with `ai-crew-sync recipes <name>`; {{input}} is where the caller's arguments go."
            );
            Ok(())
        }
        Some(n) => match find(n) {
            Some(r) => {
                print!("{}", body(r.text));
                Ok(())
            }
            None => anyhow::bail!(
                "no recipe named '{n}'. `ai-crew-sync recipes` lists them: {}",
                RECIPES
                    .iter()
                    .map(|r| r.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    #[test]
    fn every_recipe_file_is_embedded_and_every_embedded_recipe_exists() {
        let mut on_disk: Vec<String> = std::fs::read_dir(root().join("recipes"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|n| n.ends_with(".md") && n != "README.md")
            .map(|n| n.trim_end_matches(".md").to_owned())
            .collect();
        on_disk.sort();
        let embedded: Vec<&str> = RECIPES.iter().map(|r| r.name).collect();
        assert_eq!(
            embedded, on_disk,
            "recipes/ and RECIPES must list the same names"
        );
        for r in RECIPES {
            assert!(slash(r.text).is_some(), "{} has no slash header", r.name);
        }
    }

    #[test]
    fn every_slash_command_is_its_recipe_rendered() {
        let dir = root().join("plugin").join("commands");
        for r in RECIPES {
            let on_disk = std::fs::read_to_string(dir.join(format!("{}.md", r.name)))
                .unwrap_or_else(|e| panic!("plugin/commands/{}.md: {e}", r.name));
            assert_eq!(
                on_disk,
                command(r.text).unwrap(),
                "plugin/commands/{}.md drifted from recipes/{}.md; run `make recipes`",
                r.name,
                r.name
            );
        }
        let strays: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|n| n.ends_with(".md") && find(n.trim_end_matches(".md")).is_none())
            .collect();
        assert!(strays.is_empty(), "commands without a recipe: {strays:?}");
    }
}
