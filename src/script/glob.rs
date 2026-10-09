//! Маски имён каналов в привязках скриптов.

/// Маска имени канала: `*` — любая последовательность символов, `?` — один символ, остальное
/// буквально. Скобки массивов (`OBJECT1.RT_PAR_F[12]`) — обычные символы, не класс, как в glob.
#[derive(Debug, Clone)]
pub struct TagGlob {
    raw: String,
    pattern: Vec<char>,
}

impl TagGlob {
    /// Маска из строки: `*` — любые символы, `?` — один, остальное буквально (в том числе `[` и `]`).
    pub fn new(glob: &str) -> Self {
        TagGlob { raw: glob.to_string(), pattern: glob.chars().collect() }
    }

    /// Подходит ли имя тега.
    pub fn matches(&self, name: &str) -> bool {
        let text: Vec<char> = name.chars().collect();
        let p = &self.pattern;
        // Классический проход с откатом к последней «звёздочке»: O(n·m), без рекурсии.
        let (mut pi, mut ti) = (0, 0);
        let (mut star, mut mark) = (None::<usize>, 0);
        while ti < text.len() {
            if pi < p.len() && (p[pi] == '?' || (p[pi] != '*' && p[pi] == text[ti])) {
                pi += 1;
                ti += 1;
            } else if pi < p.len() && p[pi] == '*' {
                star = Some(pi);
                pi += 1;
                mark = ti;
            } else if let Some(s) = star {
                pi = s + 1;
                mark += 1;
                ti = mark;
            } else {
                return false;
            }
        }
        while pi < p.len() && p[pi] == '*' {
            pi += 1;
        }
        pi == p.len()
    }
}

impl std::fmt::Display for TagGlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.raw)
    }
}
