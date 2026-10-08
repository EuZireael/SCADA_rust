//! Значение тега и его тип.

/// Значение тега в симуляторе.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl Value {
    /// Значение числом: bool — 0/1, строка — `None`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Bool(b) => Some(f64::from(u8::from(*b))),
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Text(_) => None,
        }
    }
}

/// Тип данных тега (`type:` в YAML).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Bool,
    Int,
    Float,
    Byte,
    String,
}

impl DataType {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "bool" => DataType::Bool,
            "int" => DataType::Int,
            "float" => DataType::Float,
            "byte" => DataType::Byte,
            "string" => DataType::String,
            _ => return None,
        })
    }

    /// Число из архива/записи → значение этого типа (bool — «не ноль», int — отбрасывание дробной части).
    pub fn of_f64(self, raw: f64) -> Value {
        match self {
            DataType::Bool => Value::Bool(raw != 0.0),
            DataType::Int => Value::Int(raw.trunc() as i64),
            DataType::Float => Value::Float(raw),
            DataType::Byte => Value::Int((raw.trunc() as i64) & 0xFF),
            DataType::String => Value::Text(float_text(raw)),
        }
    }

    /// Значение любого вида → значение этого типа; None — не приводится (строка в число).
    pub fn convert(self, value: &Value) -> Option<Value> {
        match value {
            Value::Text(s) => match self {
                DataType::String => Some(Value::Text(s.clone())),
                _ => s.trim().parse::<f64>().ok().map(|f| self.of_f64(f)),
            },
            other => {
                let raw = other.as_f64()?;
                match (self, other) {
                    (DataType::String, Value::Int(i)) => Some(Value::Text(i.to_string())),
                    (DataType::String, Value::Bool(b)) => Some(Value::Text(if *b { "True" } else { "False" }.into())),
                    _ => Some(self.of_f64(raw)),
                }
            }
        }
    }

    /// Нулевое значение типа.
    pub fn zero(self) -> Value {
        self.of_f64(0.0)
    }
}

/// Как Python печатает число в `str()`: целое значение float — с «.0».
fn float_text(f: f64) -> String {
    if f.is_finite() && f.fract() == 0.0 { format!("{f:.1}") } else { format!("{f}") }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_are_converted_like_the_plc_does() {
        assert_eq!(DataType::Int.of_f64(2.9), Value::Int(2));
        assert_eq!(DataType::Int.of_f64(-2.9), Value::Int(-2));
        assert_eq!(DataType::Bool.of_f64(0.0), Value::Bool(false));
        assert_eq!(DataType::Bool.of_f64(0.1), Value::Bool(true));
        assert_eq!(DataType::Byte.of_f64(257.0), Value::Int(1));
        assert_eq!(DataType::Float.of_f64(1.5), Value::Float(1.5));
        assert_eq!(DataType::String.of_f64(3.0), Value::Text("3.0".into()));
    }

    #[test]
    fn written_values_are_converted_or_rejected() {
        assert_eq!(DataType::Float.convert(&Value::Int(3)), Some(Value::Float(3.0)));
        assert_eq!(DataType::Int.convert(&Value::Text("7".into())), Some(Value::Int(7)));
        assert_eq!(DataType::Int.convert(&Value::Text("abc".into())), None);
        assert_eq!(DataType::String.convert(&Value::Int(5)), Some(Value::Text("5".into())));
        assert_eq!(DataType::Bool.convert(&Value::Int(1)), Some(Value::Bool(true)));
    }
}
