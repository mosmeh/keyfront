use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs::File,
    hash::Hash,
    io::{BufReader, BufWriter, Write},
    path::Path,
};

const SRC_DIR: &str = "valkey/src";

fn main() {
    assert!(
        Path::new(SRC_DIR).exists(),
        "Missing Valkey source directory. Run `git submodule update --init --recursive`"
    );
    let out_dir = std::env::var_os("OUT_DIR").unwrap();
    let out_dir = Path::new(&out_dir);
    generate_version(out_dir);
    generate_commands(out_dir);
    println!("cargo::rerun-if-changed={SRC_DIR}");
}

fn generate_version(out_dir: &Path) {
    let content = std::fs::read_to_string(Path::new(SRC_DIR).join("version.h")).unwrap();
    let version = content
        .lines()
        .find_map(|line| line.trim_start().strip_prefix("#define REDIS_VERSION"))
        .unwrap()
        .trim()
        .strip_prefix('"')
        .unwrap()
        .strip_suffix('"')
        .unwrap();
    std::fs::write(
        out_dir.join("version.rs"),
        format!("pub const COMPATIBLE_VERSION: &str = \"{version}\";\n"),
    )
    .unwrap();
}

fn generate_commands(out_dir: &Path) {
    let mut paths = std::fs::read_dir(Path::new(SRC_DIR).join("commands"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect::<Vec<_>>();
    paths.sort_by(|a, b| a.file_stem().cmp(&b.file_stem()));

    let mut commands = BTreeMap::<CommandName, Command>::new();
    for path in paths {
        let reader = BufReader::new(File::open(&path).unwrap());
        let schema: BTreeMap<CommandName, RawCommand> = serde_json::from_reader(reader).unwrap();
        for (name, raw_command) in schema {
            let container = raw_command.container.clone();
            let command = Command::from_raw(name.clone(), container.clone(), raw_command);
            let prev = if let Some(container) = container {
                commands
                    .get_mut(&container)
                    .unwrap()
                    .subcommands
                    .insert(name, command.shift_args(2)) // Skip container and command name
            } else {
                commands.insert(name, command.shift_args(1)) // Skip command name
            };
            assert!(prev.is_none());
        }
    }
    {
        let name = CommandName("keyfront".to_owned());
        let prev = commands.insert(name.clone(), Command::new(name, Arity::AtLeast(1)));
        assert!(prev.is_none());
    }

    let mut writer = BufWriter::new(File::create(out_dir.join("commands.rs")).unwrap());

    writer
        .write_all(
            b"#[allow(dead_code)]
        pub enum CommandId {",
        )
        .unwrap();
    for (name, command) in &commands {
        let ident = name.to_identifier();
        if command.subcommands.is_empty() {
            writeln!(&mut writer, "{ident},").unwrap();
        } else {
            writeln!(&mut writer, "{ident}({ident}Command),").unwrap();
        }
    }
    writer.write_all(b"}\n").unwrap();

    for (name, command) in &commands {
        if command.subcommands.is_empty() {
            continue;
        }
        writeln!(
            &mut writer,
            "#[derive(Clone, Copy)]
            pub enum {}Command {{
                Container,",
            name.to_identifier()
        )
        .unwrap();
        for sub_name in command.subcommands.keys() {
            writeln!(&mut writer, "{},", sub_name.to_identifier()).unwrap();
        }
        writer.write_all(b"}\n").unwrap();
    }

    let mut map = phf_codegen::Map::new();
    for (name, command) in commands {
        map.entry(name, format!("{command:?}"));
    }
    writeln!(
        &mut writer,
        "pub static COMMANDS: phf::Map<CommandName, Command> = {};",
        map.build()
    )
    .unwrap();

    writer.flush().unwrap();
}

struct Command {
    name: CommandName,
    container: Option<CommandName>,
    arity: Arity,
    key_spec: Option<KeySpec>,
    subcommands: BTreeMap<CommandName, Command>,
}

impl Command {
    fn new(name: CommandName, arity: Arity) -> Self {
        Self {
            name,
            container: None,
            arity,
            key_spec: None,
            subcommands: BTreeMap::new(),
        }
    }

    fn from_raw(name: CommandName, container: Option<CommandName>, raw: RawCommand) -> Self {
        Self {
            name,
            container,
            arity: Arity::new(raw.arity),
            key_spec: KeySpec::from_raw(&raw),
            subcommands: BTreeMap::new(),
        }
    }

    fn shift_args(mut self, n: usize) -> Self {
        self.arity.shift_args(n);
        if let Some(key_spec) = &mut self.key_spec {
            key_spec.shift_args(n);
        }
        self
    }
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id = if let Some(container) = &self.container {
            let container = container.to_identifier();
            format!(
                "CommandId::{}({}Command::{})",
                container,
                container,
                self.name.to_identifier()
            )
        } else if self.subcommands.is_empty() {
            format!("CommandId::{}", self.name.to_identifier())
        } else {
            let container = self.name.to_identifier();
            format!("CommandId::{container}({container}Command::Container)")
        };

        let full_name = if let Some(container) = &self.container {
            format!("{}|{}", container, self.name)
        } else {
            self.name.to_string()
        };

        let mut subcommands_map = phf_codegen::Map::new();
        for (name, command) in &self.subcommands {
            subcommands_map.entry(name, format!("{command:?}"));
        }

        write!(
            f,
            "Command {{
                id: {},
                full_name: {:?},
                arity: {:?},
                key_spec: {:?},
                subcommands: {}
            }}",
            id,
            full_name,
            self.arity,
            self.key_spec,
            subcommands_map.build()
        )
    }
}

enum Arity {
    Exactly(usize),
    AtLeast(usize),
}

impl std::fmt::Debug for Arity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exactly(n) => write!(f, "Arity::Exactly({n})"),
            Self::AtLeast(n) => write!(f, "Arity::AtLeast({n})"),
        }
    }
}

impl Arity {
    fn new(n: isize) -> Self {
        assert!(n != 0);
        if n > 0 {
            Self::Exactly(n.cast_unsigned())
        } else {
            Self::AtLeast((-n).cast_unsigned())
        }
    }

    fn shift_args(&mut self, n: usize) {
        match self {
            Self::Exactly(m) | Self::AtLeast(m) => *m -= n,
        }
    }
}

#[derive(Debug)]
#[expect(dead_code)]
struct KeySpec {
    first: usize,
    last: isize,
    step: usize,
}

impl KeySpec {
    fn from_raw(raw: &RawCommand) -> Option<Self> {
        match raw.key_specs.as_slice() {
            [] => None,
            &[
                RawKeySpec {
                    begin_search: BeginSearch::Index { pos },
                    find_keys: FindKeys::Range { last, step, limit },
                },
            ] => {
                assert!(limit == 0);
                Some(Self {
                    first: pos,
                    last,
                    step,
                })
            }
            specs => {
                let mut merged_first: Option<usize> = None;
                let mut merged_last: Option<isize> = None;
                let mut prev_merged_last: Option<isize> = None;
                for spec in specs {
                    let &RawKeySpec {
                        begin_search: BeginSearch::Index { pos },
                        find_keys:
                            FindKeys::Range {
                                last,
                                step: 1,
                                limit,
                            },
                    } = spec
                    else {
                        return None;
                    };
                    if prev_merged_last.is_some_and(|x| x != pos.cast_signed() - 1) {
                        return None;
                    }
                    assert!(limit == 0);

                    merged_first = Some(merged_first.map_or(pos, |x| x.min(pos)));

                    let abs_last = if last >= 0 {
                        last + pos.cast_signed()
                    } else {
                        last
                    };
                    merged_last = Some(match merged_last {
                        None => abs_last,
                        Some(x) => x
                            .cast_unsigned()
                            .max(abs_last.cast_unsigned())
                            .cast_signed(),
                    });
                    prev_merged_last = merged_last;
                }

                let first = merged_first.unwrap();
                let last = merged_last.unwrap();
                Some(Self {
                    first,
                    last: if last < 0 {
                        last
                    } else {
                        last - first.cast_signed()
                    },
                    step: 1,
                })
            }
        }
    }

    fn shift_args(&mut self, n: usize) {
        self.first -= n;
    }
}

#[derive(Deserialize)]
struct RawCommand {
    container: Option<CommandName>,
    arity: isize,
    #[serde(default)]
    key_specs: Vec<RawKeySpec>,
}

#[derive(Deserialize)]
struct RawKeySpec {
    begin_search: BeginSearch,
    find_keys: FindKeys,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum BeginSearch {
    Index { pos: usize },
    Keyword {},
    Unknown,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum FindKeys {
    Range {
        #[serde(rename = "lastkey")]
        last: isize,
        step: usize,
        limit: usize,
    },
    KeyNum {},
    Unknown,
}

#[derive(Clone, Deserialize)]
#[serde(transparent)]
struct CommandName(String);

impl std::fmt::Display for CommandName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.to_ascii_lowercase().fmt(f)
    }
}

impl PartialEq for CommandName {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Eq for CommandName {}

impl Ord for CommandName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .to_ascii_lowercase()
            .cmp(&other.0.to_ascii_lowercase())
    }
}

impl PartialOrd for CommandName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for CommandName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for b in self.0.as_bytes() {
            state.write_u8(b.to_ascii_lowercase());
        }
    }
}

impl phf_shared::PhfHash for CommandName {
    fn phf_hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl phf_shared::FmtConst for CommandName {
    fn fmt_const(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommandName(b{:?})", self.0.to_ascii_lowercase())
    }
}

impl CommandName {
    fn to_identifier(&self) -> String {
        // To UpperCamelCase
        let mut ident = String::new();
        for word in self.0.to_ascii_lowercase().split(['_', '-']) {
            let mut chars = word.chars();
            ident.push(chars.next().unwrap().to_ascii_uppercase());
            ident.extend(chars);
        }
        ident
    }
}
