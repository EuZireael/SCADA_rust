//! Мини-разбор снимка PAC: `t=…` и `t.ИМЯ = …` — таблицы Lua с числами и строками. Нужен проверке соответствия
//! (`conformance.rs`), которая читает значения полей приборов у любого PAC.
//!
//! ```text
//! t=
//!     {
//!     LINE1V0={M=0, ST=1},
//!     }
//! t.OBJECT1 = t.OBJECT1 or {}
//! t.OBJECT1= { CMD=0, CUR_REC='Танк', RT_PAR_F={[7]=1.25, [12]=0.5} }
//! ```

use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, bail, ensure};

#[derive(Debug, Clone, PartialEq)]
pub enum Lua {
    Num(f64),
    Str(String),
    Table(BTreeMap<Key, Lua>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Key {
    Index(i64),
    Name(String),
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Num(f64),
    Str(String),
    Name(String),
    /// `[12]`
    Index(i64),
    Punct(char),
}

fn tokens(s: &str) -> Result<Vec<Tok>> {
    let chars: Vec<char> = s.chars().collect();
    let (mut i, mut out) = (0, Vec::new());
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if matches!(c, '{' | '}' | '=' | ',' | '.') {
            out.push(Tok::Punct(c));
            i += 1;
        } else if c == '[' && chars.get(i + 1).is_some_and(|q| matches!(q, '\'' | '"')) {
            // ["1V1"] — имя, которое не идентификатор Lua (прибор с цифры).
            let quote = chars[i + 1];
            let end = chars[i + 2..].iter().position(|&x| x == quote).map(|p| i + 2 + p);
            let Some(end) = end else { bail!("lua: строка не закрыта") };
            ensure!(chars.get(end + 1) == Some(&']'), "lua: ожидалась ] после имени");
            out.push(Tok::Name(chars[i + 2..end].iter().collect()));
            i = end + 2;
        } else if c == '[' {
            let end = chars[i..].iter().position(|&x| x == ']').map(|p| i + p);
            let Some(end) = end else { bail!("lua: нет ] после [") };
            let digits: String = chars[i + 1..end].iter().filter(|c| !c.is_whitespace()).collect();
            out.push(Tok::Index(digits.parse()?));
            i = end + 1;
        } else if c == '\'' || c == '"' {
            let mut j = i + 1;
            let mut text = String::new();
            while j < chars.len() && chars[j] != c {
                if chars[j] == '\\' && j + 1 < chars.len() {
                    j += 1;
                }
                text.push(chars[j]);
                j += 1;
            }
            ensure!(j < chars.len(), "lua: строка не закрыта");
            out.push(Tok::Str(text));
            i = j + 1;
        } else if c.is_ascii_digit() || c == '-' {
            let mut j = i + 1;
            while j < chars.len() && (chars[j].is_ascii_digit() || matches!(chars[j], '.' | 'e' | 'E' | '+' | '-')) {
                j += 1;
            }
            out.push(Tok::Num(chars[i..j].iter().collect::<String>().parse()?));
            i = j;
        } else if c.is_alphabetic() || c == '_' {
            let mut j = i;
            while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            out.push(Tok::Name(chars[i..j].iter().collect()));
            i = j;
        } else {
            bail!("lua: неожиданный символ {c:?}");
        }
    }
    Ok(out)
}

fn table(toks: &[Tok], mut i: usize) -> Result<(Lua, usize)> {
    ensure!(toks.get(i) == Some(&Tok::Punct('{')), "lua: ожидалась {{");
    i += 1;
    let (mut map, mut positional) = (BTreeMap::new(), 0i64);
    while toks.get(i).is_some_and(|t| *t != Tok::Punct('}')) {
        if toks[i] == Tok::Punct(',') {
            i += 1;
            continue;
        }
        let key = match (&toks[i], toks.get(i + 1)) {
            (Tok::Name(n), Some(Tok::Punct('='))) => {
                i += 2;
                Some(Key::Name(n.clone()))
            }
            (Tok::Index(n), Some(Tok::Punct('='))) => {
                i += 2;
                Some(Key::Index(*n))
            }
            _ => None,
        };
        let value = match toks.get(i) {
            Some(Tok::Punct('{')) => {
                let (v, next) = table(toks, i)?;
                i = next;
                v
            }
            Some(Tok::Num(n)) => {
                i += 1;
                Lua::Num(*n)
            }
            Some(Tok::Str(s)) => {
                i += 1;
                Lua::Str(s.clone())
            }
            other => bail!("lua: неожиданный элемент таблицы {other:?}"),
        };
        let key = key.unwrap_or_else(|| {
            positional += 1;
            Key::Index(positional)
        });
        map.insert(key, value);
    }
    ensure!(i < toks.len(), "lua: таблица не закрыта");
    Ok((Lua::Table(map), i + 1))
}

/// Снимок: имя прибора → его таблица.
pub type Snapshot = HashMap<String, BTreeMap<Key, Lua>>;

pub fn parse_snapshot(text: &str) -> Result<Snapshot> {
    let toks = tokens(text)?;
    let (mut i, mut out) = (0, Snapshot::new());
    while i < toks.len() {
        ensure!(toks[i] == Tok::Name("t".into()), "lua: ожидалось t, а не {:?}", toks[i]);
        i += 1;
        match toks.get(i) {
            Some(Tok::Punct('=')) => {
                let (Lua::Table(devices), next) = table(&toks, i + 1)? else { bail!("lua: t не таблица") };
                for (k, v) in devices {
                    if let (Key::Name(name), Lua::Table(fields)) = (k, v) {
                        out.entry(name).or_default().extend(fields);
                    }
                }
                i = next;
            }
            Some(Tok::Punct('.')) => {
                let Some(Tok::Name(name)) = toks.get(i + 1) else {
                    bail!("lua: после t. ожидалось имя")
                };
                ensure!(toks.get(i + 2) == Some(&Tok::Punct('=')), "lua: ожидалось =");
                i += 3;
                if toks.get(i) == Some(&Tok::Name("t".into())) {
                    // t.NAME = t.NAME or {}
                    while toks.get(i).is_some_and(|t| *t != Tok::Punct('}')) {
                        i += 1;
                    }
                    i += 1;
                } else {
                    let (Lua::Table(fields), next) = table(&toks, i)? else { bail!("lua: нужна таблица") };
                    out.entry(name.clone()).or_default().extend(fields);
                    i = next;
                }
            }
            other => bail!("lua: после t ожидалось = или ., а не {other:?}"),
        }
    }
    Ok(out)
}

/// Число поля прибора: `device.base` или `device.base[index]`; строки и таблицы — None.
pub fn number(snapshot: &Snapshot, device: &str, base: &str, index: Option<i64>) -> Option<f64> {
    let v = snapshot.get(device)?.get(&Key::Name(base.to_string()))?;
    let v = match index {
        Some(i) => match v {
            Lua::Table(t) => t.get(&Key::Index(i))?,
            _ => return None,
        },
        None => v,
    };
    match v {
        Lua::Num(n) => Some(*n),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SNAPSHOT: &str = "t=\n\t{\n\tLINE1V0={M=0, ST=1},\n\t[\"1V1\"]={ST=1},\n\t}\n\
        t.OBJECT1 = t.OBJECT1 or {}\nt.OBJECT1=\n\t{\n\tCMD=0,\n\tCUR_REC='Танк №1 \\'сырой\\'',\n\
        \tRT_PAR_F={[7]=1.25, [12]=0.5},\n\tPAR_MAIN=\n\t\t{\n\t\t1, 0.20, 1e-3, \n\t\t},\n\t}\n";

    #[test]
    fn parses_devices_arrays_and_strings() {
        let s = parse_snapshot(SNAPSHOT).unwrap();
        assert_eq!(number(&s, "1V1", "ST", None), Some(1.0), "прибор с именем не из идентификатора");
        assert_eq!(number(&s, "LINE1V0", "ST", None), Some(1.0));
        assert_eq!(number(&s, "OBJECT1", "RT_PAR_F", Some(12)), Some(0.5));
        assert_eq!(number(&s, "OBJECT1", "PAR_MAIN", Some(2)), Some(0.2), "позиционные элементы нумеруются с 1");
        assert_eq!(number(&s, "OBJECT1", "PAR_MAIN", Some(3)), Some(0.001));
        assert_eq!(number(&s, "OBJECT1", "CUR_REC", None), None, "строка — не число");
        assert_eq!(number(&s, "NO", "ST", None), None);
        assert_eq!(number(&s, "LINE1V0", "ST", Some(1)), None, "скаляр — не массив");
    }

    #[test]
    fn broken_text_is_an_error() {
        assert!(parse_snapshot("t={LINE1V0={ST=1}").is_err());
        assert!(parse_snapshot("x=1").is_err());
    }
}
