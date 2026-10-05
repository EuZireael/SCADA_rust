//! Движок воспроизведения (replay) архива тегов BN1_MCA1.
//!
//! Архив — событийный: каждая запись это изменение значения тега в момент времени. Воспроизведение
//! держит последнее значение (zero-order hold): в модельный момент T тег держит значение последней
//! записи с t <= T. Время архива (5 суток) проецируется на реальное с коэффициентом `speed` и
//! (опционально) зацикливается.
//!
//! Сырая серия историана не всегда годится каналу как есть: датчик в обрыве пишет код ±3276.7,
//! у закрытого клапана смещение нуля даёт −2 %, счётчику объёма нужен интеграл расхода, а «время
//! текущей операции» — возраст последнего изменения номера операции. Это описывает [`ReplaySpec`]
//! тега, а [`Replay::evaluate`] применяет его к серии.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result, bail, ensure};

const MAGIC: &[u8; 8] = b"SIMARC01";

/// Что берётся из серии.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Значение серии (или on/off по порогу).
    #[default]
    Value,
    /// Интеграл по времени в секундах (счётчик объёма, моточасы).
    Integral,
    /// Возраст последнего изменения в секундах.
    Age,
}

impl Mode {
    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "value" => Mode::Value,
            "integral" => Mode::Integral,
            "age" => Mode::Age,
            other => bail!("replay_mode={other:?}, ожидается value, integral или age"),
        })
    }
}

/// Как канал получает значение из серии архива.
///
/// Порядок: из серии выбрасываются точки вне [valid_min, valid_max] (код обрыва датчика — канал
/// держит последнее нормальное значение); затем по режиму берётся значение, интеграл по времени в
/// секундах или возраст последнего изменения. Если задан порог `above`, серия служит условием
/// «активно, когда >= above»: значение даёт on/off, интеграл копит on/off, возраст сбрасывается в
/// `off`, пока условие не выполнено. Результат масштабируется (scale, bias) и обрезается по
/// [clamp_min, clamp_max].
#[derive(Debug, Clone, PartialEq)]
pub struct ReplaySpec {
    pub source: String,
    pub offset: f64,
    pub mode: Mode,
    pub valid_min: Option<f64>,
    pub valid_max: Option<f64>,
    pub above: Option<f64>,
    pub on: f64,
    pub off: f64,
    pub scale: f64,
    pub bias: f64,
    pub clamp_min: Option<f64>,
    pub clamp_max: Option<f64>,
}

impl ReplaySpec {
    #[cfg(test)]
    pub fn new(source: impl Into<String>) -> Self {
        ReplaySpec {
            source: source.into(),
            offset: 0.0,
            mode: Mode::Value,
            valid_min: None,
            valid_max: None,
            above: None,
            on: 1.0,
            off: 0.0,
            scale: 1.0,
            bias: 0.0,
            clamp_min: None,
            clamp_max: None,
        }
    }
}

/// Серия архива: времена (секунды от начала архива, по возрастанию) и значения.
#[derive(Debug, Clone, Default)]
pub struct Series {
    pub t: Vec<f64>,
    pub v: Vec<f64>,
}

/// Архив, прочитанный из файла.
#[derive(Debug, Default)]
pub struct Archive {
    #[allow(dead_code)] // момент начала архива в календаре; replay считает от нуля архива
    pub start_epoch: f64,
    pub duration: f64,
    pub series: HashMap<String, Series>,
}

impl Archive {
    /// Читает `data/archive_replay.bin.gz` (формат — tools/convert_archive.py).
    pub fn load(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path).with_context(|| format!("архив replay не найден: {}", path.display()))?;
        let mut raw = Vec::new();
        flate2::read::GzDecoder::new(file).read_to_end(&mut raw).context("архив replay: не gzip")?;
        Self::parse(&raw)
    }

    pub fn parse(raw: &[u8]) -> Result<Self> {
        let mut r = Cursor { buf: raw, pos: 0 };
        ensure!(r.take(8)? == MAGIC, "архив replay: неверная сигнатура (ожидается SIMARC01)");
        let start_epoch = f64::from_le_bytes(r.take(8)?.try_into()?);
        let duration = f64::from_le_bytes(r.take(8)?.try_into()?);
        let count = u32::from_le_bytes(r.take(4)?.try_into()?) as usize;
        let mut series = HashMap::with_capacity(count);
        for _ in 0..count {
            let id_len = u16::from_le_bytes(r.take(2)?.try_into()?) as usize;
            let id = String::from_utf8(r.take(id_len)?.to_vec()).context("архив replay: id серии не UTF-8")?;
            let n = u32::from_le_bytes(r.take(4)?.try_into()?) as usize;
            let floats = |bytes: &[u8]| -> Vec<f64> {
                bytes.as_chunks::<4>().0.iter().map(|c| f64::from(f32::from_le_bytes(*c))).collect()
            };
            let t = floats(r.take(n * 4)?);
            let v = floats(r.take(n * 4)?);
            ensure!(n > 0, "архив replay: пустая серия {id}");
            series.insert(id, Series { t, v });
        }
        ensure!(r.pos == raw.len(), "архив replay: лишние данные в конце файла");
        Ok(Archive { start_epoch, duration, series })
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.buf.len());
        let Some(end) = end else { bail!("архив replay: файл обрезан") };
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FilterKey {
    source: String,
    vmin: Option<u64>,
    vmax: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CumKey {
    filter: FilterKey,
    above: Option<u64>,
    on: u64,
    off: u64,
}

fn bits(v: Option<f64>) -> Option<u64> {
    v.map(f64::to_bits)
}

/// Интеграл серии: cum[i] — интеграл от 0 до t[i]; до первой записи действует w[0].
struct Cumulative {
    t: Vec<f64>,
    w: Vec<f64>,
    cum: Vec<f64>,
    total: f64,
}

/// Проигрыватель архива: по модельному времени отдаёт значение канала по его [`ReplaySpec`].
pub struct Replay {
    pub duration: f64,
    pub speed: f64,
    pub looped: bool,
    series: HashMap<String, Series>,
    filtered: Mutex<HashMap<FilterKey, Option<Arc<Series>>>>,
    cumulative: Mutex<HashMap<CumKey, Arc<Cumulative>>>,
    wall_start: Instant,
}

impl Replay {
    pub fn new(archive: Archive, speed: f64, looped: bool) -> Self {
        Replay {
            duration: archive.duration,
            speed,
            looped,
            series: archive.series,
            filtered: Mutex::new(HashMap::new()),
            cumulative: Mutex::new(HashMap::new()),
            wall_start: Instant::now(),
        }
    }

    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    #[cfg(test)]
    pub fn has(&self, id: &str) -> bool {
        self.series.contains_key(id)
    }

    /// Модельное время с начала воспроизведения, секунды, без зацикливания.
    pub fn elapsed(&self) -> f64 {
        self.wall_start.elapsed().as_secs_f64() * self.speed
    }

    /// Позиция внутри архива для модельного времени.
    pub fn position(&self, elapsed: f64) -> f64 {
        if self.looped && self.duration > 0.0 { elapsed.rem_euclid(self.duration) } else { elapsed.min(self.duration) }
    }

    /// Значение канала в модельный момент `elapsed` (None — серии нет).
    pub fn evaluate(&self, spec: &ReplaySpec, elapsed: f64) -> Option<f64> {
        let series = self.filtered_series(&spec.source, spec.valid_min, spec.valid_max)?;
        let shifted = elapsed + spec.offset;
        let pos = self.position(shifted);
        // До первой записи держим первое значение.
        let current = series.v[count_le(&series.t, pos).saturating_sub(1)];
        let active = spec.above.is_none_or(|a| current >= a);

        let raw = match spec.mode {
            Mode::Integral => self.integral(spec, shifted)?,
            Mode::Age => {
                if active {
                    self.age(&series.t, pos)
                } else {
                    spec.off
                }
            }
            Mode::Value => match spec.above {
                Some(_) => {
                    if active {
                        spec.on
                    } else {
                        spec.off
                    }
                }
                None => current,
            },
        };

        let mut result = raw * spec.scale + spec.bias;
        if let Some(min) = spec.clamp_min {
            result = result.max(min);
        }
        if let Some(max) = spec.clamp_max {
            result = result.min(max);
        }
        Some(result)
    }

    fn filtered_series(&self, source: &str, vmin: Option<f64>, vmax: Option<f64>) -> Option<Arc<Series>> {
        let key = FilterKey { source: source.to_string(), vmin: bits(vmin), vmax: bits(vmax) };
        let mut cache = self.filtered.lock().expect("кэш серий");
        cache
            .entry(key)
            .or_insert_with(|| {
                let series = self.series.get(source)?;
                if vmin.is_none() && vmax.is_none() {
                    return Some(Arc::new(series.clone()));
                }
                let mut out = Series::default();
                for (&t, &v) in series.t.iter().zip(&series.v) {
                    if vmin.is_none_or(|m| v >= m) && vmax.is_none_or(|m| v <= m) {
                        out.t.push(t);
                        out.v.push(v);
                    }
                }
                (!out.t.is_empty()).then(|| Arc::new(out))
            })
            .clone()
    }

    fn age(&self, t: &[f64], pos: f64) -> f64 {
        let count = count_le(t, pos);
        if count > 0 {
            return pos - t[count - 1];
        }
        // Позиция раньше первого изменения — отсчёт идёт с последнего изменения предыдущего круга.
        if self.looped { pos + (self.duration - t[t.len() - 1]) } else { pos }
    }

    fn integral(&self, spec: &ReplaySpec, elapsed: f64) -> Option<f64> {
        let cum = self.cumulative_for(spec)?;
        let (loops, pos) = if self.looped && self.duration > 0.0 {
            let e = elapsed.max(0.0);
            let pos = e % self.duration;
            (((e - pos) / self.duration).round(), pos)
        } else {
            (0.0, elapsed.max(0.0).min(self.duration))
        };
        let count = count_le(&cum.t, pos);
        let part =
            if count == 0 { cum.w[0] * pos } else { cum.cum[count - 1] + cum.w[count - 1] * (pos - cum.t[count - 1]) };
        Some(loops * cum.total + part)
    }

    fn cumulative_for(&self, spec: &ReplaySpec) -> Option<Arc<Cumulative>> {
        let key = CumKey {
            filter: FilterKey { source: spec.source.clone(), vmin: bits(spec.valid_min), vmax: bits(spec.valid_max) },
            above: bits(spec.above),
            on: spec.on.to_bits(),
            off: spec.off.to_bits(),
        };
        if let Some(c) = self.cumulative.lock().expect("кэш интегралов").get(&key) {
            return Some(c.clone());
        }
        let series = self.filtered_series(&spec.source, spec.valid_min, spec.valid_max)?;
        let w: Vec<f64> = match spec.above {
            None => series.v.clone(),
            Some(a) => series.v.iter().map(|&v| if v >= a { spec.on } else { spec.off }).collect(),
        };
        let t = series.t.clone();
        let mut cum = vec![0.0; t.len()];
        cum[0] = w[0] * t[0];
        let mut acc = 0.0;
        for k in 0..t.len() - 1 {
            acc += w[k] * (t[k + 1] - t[k]);
            cum[k + 1] = cum[0] + acc;
        }
        let last = t.len() - 1;
        let total = cum[last] + w[last] * (self.duration - t[last]);
        let built = Arc::new(Cumulative { t, w, cum, total });
        self.cumulative.lock().expect("кэш интегралов").insert(key, built.clone());
        Some(built)
    }
}

/// Сколько точек времени <= pos (numpy `searchsorted(side="right")`).
fn count_le(t: &[f64], pos: f64) -> usize {
    t.partition_point(|&x| x <= pos)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Серия синтетическая, 0..100 с. Точки (t, v): 0 → 10, 20 → 3276.7 (код обрыва датчика),
    /// 40 → 30, 70 → 0. Архив длится 100 с и зацикливается.
    fn replay() -> Replay {
        let mut series = HashMap::new();
        series.insert("S".to_string(), Series { t: vec![0.0, 20.0, 40.0, 70.0], v: vec![10.0, 3276.7, 30.0, 0.0] });
        Replay::new(Archive { start_epoch: 0.0, duration: 100.0, series }, 1.0, true)
    }

    fn spec() -> ReplaySpec {
        ReplaySpec::new("S")
    }

    fn approx(a: Option<f64>, b: f64) {
        let a = a.expect("значение");
        assert!((a - b).abs() < 1e-9, "{a} != {b}");
    }

    #[test]
    fn value_is_zero_order_hold() {
        let r = replay();
        assert_eq!(r.evaluate(&spec(), 5.0), Some(10.0));
        assert_eq!(r.evaluate(&spec(), 45.0), Some(30.0));
    }

    #[test]
    fn valid_range_holds_last_good_value_instead_of_sensor_fault() {
        // В 25 с сырое значение — код обрыва; канал держит предыдущее нормальное (10).
        let r = replay();
        assert_eq!(r.evaluate(&spec(), 25.0), Some(3276.7));
        let s = ReplaySpec { valid_min: Some(0.0), valid_max: Some(120.0), ..spec() };
        assert_eq!(r.evaluate(&s, 25.0), Some(10.0));
    }

    #[test]
    fn threshold_gives_on_off() {
        let r = replay();
        let s = ReplaySpec { valid_max: Some(120.0), above: Some(5.0), on: 75.0, off: 0.0, ..spec() };
        assert_eq!(r.evaluate(&s, 45.0), Some(75.0));
        assert_eq!(r.evaluate(&s, 80.0), Some(0.0));
    }

    #[test]
    fn scale_bias_and_clamp() {
        let r = replay();
        let s = ReplaySpec { valid_max: Some(120.0), scale: 0.5, bias: 1.0, ..spec() };
        assert_eq!(r.evaluate(&s, 45.0), Some(16.0));
        let s = ReplaySpec { valid_max: Some(120.0), scale: 10.0, clamp_max: Some(100.0), ..spec() };
        assert_eq!(r.evaluate(&s, 45.0), Some(100.0));
    }

    #[test]
    fn negative_offset_of_closed_valve_is_clamped_to_zero() {
        let mut series = HashMap::new();
        series.insert("V".to_string(), Series { t: vec![0.0], v: vec![-2.31] });
        let r = Replay::new(Archive { start_epoch: 0.0, duration: 100.0, series }, 1.0, true);
        let s = ReplaySpec { clamp_min: Some(0.0), clamp_max: Some(100.0), ..ReplaySpec::new("V") };
        assert_eq!(r.evaluate(&s, 10.0), Some(0.0));
    }

    #[test]
    fn integral_accumulates_and_never_resets_on_loop() {
        let r = replay();
        let s = ReplaySpec { mode: Mode::Integral, valid_max: Some(120.0), ..spec() };
        // Отфильтрованная серия: 0→10, 40→30, 70→0. За круг: 10*40 + 30*30 + 0*30 = 1300.
        approx(r.evaluate(&s, 40.0), 400.0);
        approx(r.evaluate(&s, 100.0), 1300.0);
        approx(r.evaluate(&s, 140.0), 1700.0);
        let values: Vec<f64> = (0..300).step_by(7).map(|x| r.evaluate(&s, f64::from(x)).unwrap()).collect();
        assert!(values.windows(2).all(|w| w[1] >= w[0]), "{values:?}");
    }

    #[test]
    fn integral_of_condition_counts_active_seconds() {
        let r = replay();
        let s =
            ReplaySpec { mode: Mode::Integral, valid_max: Some(120.0), above: Some(20.0), on: 1.0, off: 0.0, ..spec() };
        approx(r.evaluate(&s, 100.0), 30.0);
    }

    #[test]
    fn age_is_seconds_since_last_change() {
        let r = replay();
        approx(r.evaluate(&ReplaySpec { mode: Mode::Age, ..spec() }, 55.0), 15.0);
        // До первой точки круга возраст считается с последней точки предыдущего круга.
        let mut series = HashMap::new();
        series.insert("late".to_string(), Series { t: vec![10.0, 60.0], v: vec![1.0, 2.0] });
        let r = Replay::new(Archive { start_epoch: 0.0, duration: 100.0, series }, 1.0, true);
        approx(r.evaluate(&ReplaySpec { mode: Mode::Age, ..ReplaySpec::new("late") }, 5.0), 45.0);
    }

    #[test]
    fn age_is_off_while_condition_inactive() {
        let r = replay();
        let s = ReplaySpec { mode: Mode::Age, valid_max: Some(120.0), above: Some(1.0), off: 0.0, ..spec() };
        approx(r.evaluate(&s, 55.0), 15.0);
        assert_eq!(r.evaluate(&s, 90.0), Some(0.0));
    }

    #[test]
    fn offset_shifts_position() {
        let r = replay();
        assert_eq!(r.evaluate(&ReplaySpec { offset: 40.0, ..spec() }, 5.0), Some(30.0));
    }

    #[test]
    fn missing_series_returns_none() {
        let r = replay();
        assert_eq!(r.evaluate(&ReplaySpec::new("nope"), 5.0), None);
        assert_eq!(r.evaluate(&ReplaySpec { valid_min: Some(5000.0), ..spec() }, 5.0), None);
    }

    #[test]
    fn position_loops_or_stops_at_the_end() {
        let r = replay();
        assert_eq!(r.position(250.0), 50.0);
        let once = Replay { looped: false, ..replay() };
        assert_eq!(once.position(250.0), 100.0);
        assert!((0.0..r.duration).contains(&r.position(r.elapsed())));
    }

    #[test]
    fn mode_rejects_unknown_name() {
        assert!(Mode::parse("sum").is_err());
        assert_eq!(Mode::parse("age").unwrap(), Mode::Age);
    }

    fn encode(series: &[(&str, &[f32], &[f32])]) -> Vec<u8> {
        let mut out = MAGIC.to_vec();
        out.extend(1_781_125_200.5f64.to_le_bytes());
        out.extend(431_992.5f64.to_le_bytes());
        out.extend((series.len() as u32).to_le_bytes());
        for (id, t, v) in series {
            out.extend((id.len() as u16).to_le_bytes());
            out.extend(id.as_bytes());
            out.extend((t.len() as u32).to_le_bytes());
            t.iter().for_each(|x| out.extend(x.to_le_bytes()));
            v.iter().for_each(|x| out.extend(x.to_le_bytes()));
        }
        out
    }

    #[test]
    fn archive_format_roundtrip_and_corruption() {
        let raw = encode(&[("2A010000", &[0.0, 5.5], &[1.0, 2.0]), ("2A010001", &[1.0], &[7.0])]);
        let a = Archive::parse(&raw).unwrap();
        assert_eq!((a.start_epoch, a.duration, a.series.len()), (1_781_125_200.5, 431_992.5, 2));
        assert_eq!(a.series["2A010000"].t, vec![0.0, 5.5]);
        assert_eq!(a.series["2A010001"].v, vec![7.0]);
        assert!(Archive::parse(&raw[..raw.len() - 1]).is_err(), "обрезанный файл");
        let mut extra = raw.clone();
        extra.push(0);
        assert!(Archive::parse(&extra).is_err(), "лишние байты");
        let mut bad = raw;
        bad[0] = b'X';
        assert!(Archive::parse(&bad).is_err(), "чужая сигнатура");
    }

    /// Настоящий архив из репозитория читается, и в нём есть серии, на которые ссылается конфиг.
    #[test]
    fn real_archive_loads() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/archive_replay.bin.gz");
        let a = Archive::load(&path).unwrap();
        assert_eq!(a.series.len(), 170);
        assert!(a.duration > 4.0 * 86400.0, "5 суток архива, а не {}", a.duration);
        assert!(a.series.values().all(|s| s.t.windows(2).all(|w| w[0] <= w[1])), "времена по возрастанию");
    }
}
