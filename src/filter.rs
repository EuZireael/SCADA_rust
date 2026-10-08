//! Фильтр значимости: решает, публиковать ли снятое значение (в Kafka — «по исключению», в
//! локальную историю — значимые точки). Контракт: `docs/TELEMETRY_BY_EXCEPTION_CONTRACT.md`;
//! на него опирается монитор (runtime), поэтому правила и их порядок менять нельзя.
//!
//! Значение уходит, если выполнено хотя бы одно, **по порядку**:
//!
//! 1. это первое значение тега (после старта шлюза или после [`Last`]-сброса);
//! 2. сменилось качество — обрыв и восстановление связи уходят сразу;
//! 3. с последней публикации прошло `max_interval` — «полная отправка» (для Kafka — `full-resend-ms`,
//!    для истории — «пульс»): потребитель знает, что тег жив, и после своего перезапуска получает
//!    значения всех тегов не дольше чем за этот период; 0 — выключено;
//! 4. иначе, если прошло меньше `min_interval`, — **нет** (антидребезг; 0 — выключен). Изменение,
//!    отрезанное окном, не теряется: оно уйдёт первым же опросом после окна, если значение не
//!    вернулось;
//! 5. значение изменилось относительно последнего **опубликованного** (не прошлого опроса — медленный
//!    дрейф внутри зоны накапливается до порога): числа — `|новое − опубликованное| > зона`,
//!    `зона = max(deadband, |опубликованное| · deadband_percent / 100)`, при нулевой зоне — любое
//!    отличие; не числа (строки, bool) — на равенство.
//!
//! **Разброс первой полной отправки.** Все теги стартуют в один момент, и без поправки полная отправка
//! наступала бы для всех сразу — «залп» из десятков тысяч сообщений раз в `full-resend-ms`. Поэтому
//! первая полная отправка каждого тега наступает через долю периода (своя у каждого тега, см.
//! [`phase`]), а не через весь период; дальше — как обычно, раз в период с последней публикации.
//! Это укладывается в контракт («не дольше чем за `full-resend-ms`»): интервал только короче.
//!
//! Интервалы считаются по часам шлюза (момент получения значения), а не по метке источника: OPC UA
//! не двигает метку, пока значение стоит, и полная отправка по ней не наступила бы никогда.

use std::time::{Duration, Instant};

use crate::config::{HistoryConfig, HistorySettings, PublishSettings};
use crate::model::{Quality, TagValue};

/// Параметры решения для одного тега.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FilterParams {
    /// Абсолютная зона нечувствительности.
    pub deadband: f64,
    /// Относительная зона, % от последнего опубликованного значения.
    pub deadband_percent: f64,
    /// Антидребезг: не чаще одной публикации за интервал; 0 — выключено.
    pub min_interval: Duration,
    /// Полная отправка / «пульс»; ноль — выключено.
    pub max_interval: Duration,
}

impl FilterParams {
    /// Параметры отправки в Kafka: общие для всех тегов, без поканальных переопределений.
    pub fn publish(s: &PublishSettings) -> Self {
        FilterParams {
            deadband: s.deadband,
            deadband_percent: s.deadband_percent,
            min_interval: s.min_interval,
            max_interval: s.full_resend,
        }
    }

    /// Параметры истории: умолчания `gateway.history.*`, поля, заданные у тега, их переопределяют.
    pub fn history(s: &HistorySettings, tag: &HistoryConfig) -> Self {
        FilterParams {
            deadband: tag.deadband.unwrap_or(s.deadband),
            deadband_percent: tag.deadband_percent.unwrap_or(s.deadband_percent),
            min_interval: tag.min_interval_ms.map(Duration::from_millis).unwrap_or(s.min_interval),
            max_interval: tag.max_interval_ms.map(Duration::from_millis).unwrap_or(s.max_interval),
        }
    }
}

/// Последнее опубликованное значение тега.
#[derive(Debug, Clone)]
pub struct Last {
    value: Option<TagValue>,
    quality: Quality,
    at: Instant,
    /// Через сколько после `at` наступает полная отправка (у первой публикации — доля периода).
    resend_after: Duration,
}

/// Доля периода полной отправки для первой публикации тега: равномерно по (0.02, 1] и стабильно для
/// пары (контроллер, номер тега). Золотое сечение даёт низкорасхождение: соседние теги не слипаются.
pub fn phase(controller: &str, slot: usize) -> f64 {
    let salt =
        controller.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3));
    let unit = ((salt % 10_000) as f64 / 10_000.0 + slot as f64 * 0.618_033_988_749_894_9).fract();
    0.02 + unit * 0.98
}

/// Решение по значению. `true` — публиковать; решение сразу запоминается как «опубликовано»
/// (`last`), поэтому вызывать только для значений, которые действительно уйдут дальше.
pub fn decide(
    params: &FilterParams,
    last: &mut Option<Last>,
    value: Option<&TagValue>,
    quality: Quality,
    now: Instant,
) -> bool {
    decide_phased(params, last, value, quality, now, 1.0)
}

/// То же, но первая полная отправка наступает через `phase` доли периода (см. [`phase`]).
pub fn decide_phased(
    params: &FilterParams,
    last: &mut Option<Last>,
    value: Option<&TagValue>,
    quality: Quality,
    now: Instant,
    phase: f64,
) -> bool {
    let first = last.is_none();
    let publish = match last.as_ref() {
        None => true,                            // 1. первое значение
        Some(l) if l.quality != quality => true, // 2. смена качества
        Some(l) => {
            let elapsed = now.saturating_duration_since(l.at);
            if !params.max_interval.is_zero() && elapsed >= l.resend_after {
                true // 3. полная отправка
            } else if !params.min_interval.is_zero() && elapsed < params.min_interval {
                false // 4. антидребезг
            } else {
                changed(params, l.value.as_ref(), value) // 5. изменилось
            }
        }
    };
    if publish {
        let resend_after = if first { params.max_interval.mul_f64(phase.clamp(0.0, 1.0)) } else { params.max_interval };
        *last = Some(Last { value: value.cloned(), quality, at: now, resend_after });
    }
    publish
}

/// Правило 5 контракта: изменилось ли значение относительно последнего опубликованного. Числа — больше зоны (при нулевой зоне — любое отличие; `NaN` равен `NaN`), остальное — на равенство.
fn changed(params: &FilterParams, previous: Option<&TagValue>, current: Option<&TagValue>) -> bool {
    match (previous, current) {
        (Some(p), Some(c)) if p.is_numeric() && c.is_numeric() => {
            let (before, after) = (p.as_f64().unwrap_or(f64::NAN), c.as_f64().unwrap_or(f64::NAN));
            match (before.is_nan(), after.is_nan()) {
                (true, true) => return false, // NaN → NaN — значение не изменилось
                (true, false) | (false, true) => return true,
                _ => {}
            }
            let delta = (after - before).abs();
            let zone = params.deadband.max(before.abs() * params.deadband_percent / 100.0);
            if zone > 0.0 { delta > zone } else { delta != 0.0 }
        }
        (p, c) => p != c,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: fn() -> Instant = Instant::now;

    fn params(deadband: f64, percent: f64, min_ms: u64, max_ms: u64) -> FilterParams {
        FilterParams {
            deadband,
            deadband_percent: percent,
            min_interval: Duration::from_millis(min_ms),
            max_interval: Duration::from_millis(max_ms),
        }
    }

    /// Прогон значений: `(через сколько мс от старта, значение, качество)` → решения.
    fn run(p: &FilterParams, steps: &[(u64, Option<TagValue>, Quality)]) -> Vec<bool> {
        let (base, mut last) = (T0(), None);
        steps.iter().map(|(ms, v, q)| decide(p, &mut last, v.as_ref(), *q, base + Duration::from_millis(*ms))).collect()
    }

    #[test]
    fn first_full_resend_comes_after_the_tag_phase_and_then_every_period() {
        let p = params(0.0, 0.0, 0, 30_000);
        let (base, mut last) = (T0(), None);
        let mut step = |ms: u64| {
            decide_phased(&p, &mut last, f(1.0).as_ref(), Quality::Good, base + Duration::from_millis(ms), 0.2)
        };
        assert!(step(0), "первое значение");
        assert!(!step(5_000));
        assert!(step(6_000), "первая полная отправка — через 0.2 периода");
        assert!(!step(35_000), "дальше — через период от последней публикации");
        assert!(step(36_000));
    }

    #[test]
    fn a_change_resets_the_resend_clock_to_the_full_period() {
        let p = params(0.0, 0.0, 0, 30_000);
        let (base, mut last) = (T0(), None);
        let mut step = |ms: u64, v: f64| {
            decide_phased(&p, &mut last, f(v).as_ref(), Quality::Good, base + Duration::from_millis(ms), 0.1)
        };
        assert!(step(0, 1.0));
        assert!(step(1_000, 2.0), "изменение");
        assert!(!step(20_000, 2.0), "после изменения полный период, а не доля");
        assert!(step(31_000, 2.0));
    }

    #[test]
    fn phases_spread_evenly_and_never_exceed_the_period() {
        let phases: Vec<f64> = (0..10_000).map(|slot| phase("Контроллер-1", slot)).collect();
        assert!(phases.iter().all(|p| (0.02..=1.0).contains(p)));
        // 10 равных долей периода: в каждой около десятой части тегов (±15 %).
        let mut buckets = [0usize; 10];
        phases.iter().for_each(|p| buckets[((p - 0.02) / 0.98 * 10.0).min(9.0) as usize] += 1);
        assert!(buckets.iter().all(|&n| (850..=1150).contains(&n)), "{buckets:?}");
        assert_ne!(phase("A", 5), phase("B", 5), "у разных контроллеров фазы разные");
        assert_eq!(phase("A", 5), phase("A", 5), "и стабильные");
    }

    #[test]
    fn staggered_resend_has_no_burst() {
        // 20 000 тегов, период 30 с: за каждую секунду первой полной отправки — не больше ~3 % тегов.
        let p = params(0.0, 0.0, 0, 30_000);
        let base = T0();
        let mut states: Vec<Option<Last>> = vec![None; 20_000];
        let phases: Vec<f64> = (0..20_000).map(|slot| phase("C", slot)).collect();
        for (st, ph) in states.iter_mut().zip(&phases) {
            assert!(decide_phased(&p, st, f(1.0).as_ref(), Quality::Good, base, *ph));
        }
        let mut worst = 0;
        for sec in 1..=30u64 {
            let mut sent = 0;
            for (st, ph) in states.iter_mut().zip(&phases) {
                let now = base + Duration::from_secs(sec);
                sent += usize::from(decide_phased(&p, st, f(1.0).as_ref(), Quality::Good, now, *ph));
            }
            worst = worst.max(sent);
        }
        assert!(worst < 20_000 * 4 / 100, "в одну секунду ушло {worst} из 20000");
    }

    fn f(v: f64) -> Option<TagValue> {
        Some(TagValue::F64(v))
    }

    fn i(v: i64) -> Option<TagValue> {
        Some(TagValue::Int(v))
    }

    use Quality::{Bad, Good};

    #[test]
    fn first_value_is_published_then_repeats_are_not() {
        let p = params(0.0, 0.0, 0, 30_000);
        assert_eq!(run(&p, &[(0, i(42), Good), (2000, i(42), Good), (4000, i(42), Good)]), [true, false, false]);
    }

    #[test]
    fn change_is_published_immediately() {
        let p = params(0.0, 0.0, 0, 30_000);
        assert_eq!(
            run(&p, &[(0, i(42), Good), (2000, i(42), Good), (4000, i(43), Good), (6000, i(43), Good)]),
            [true, false, true, false]
        );
    }

    #[test]
    fn unchanged_value_is_resent_every_full_resend_interval() {
        let p = params(0.0, 0.0, 0, 30_000);
        let steps: Vec<_> = (0..=7).map(|k| (k * 10_000, i(5), Good)).collect();
        // t=0 первое; 10, 20 с — нет; 30 с — полная отправка (ровно граница включена); дальше снова с 30 с.
        assert_eq!(run(&p, &steps), [true, false, false, true, false, false, true, false]);
    }

    #[test]
    fn full_resend_timer_restarts_on_a_changed_publication() {
        // Изменение — тоже публикация и сдвигает отсчёт: полная отправка — через 30 с после неё.
        let p = params(0.0, 0.0, 0, 30_000);
        assert_eq!(
            run(&p, &[(0, i(1), Good), (20_000, i(2), Good), (49_000, i(2), Good), (50_000, i(2), Good)]),
            [true, true, false, true]
        );
    }

    #[test]
    fn quality_change_is_published_at_once_both_ways() {
        let p = params(0.0, 0.0, 10_000, 30_000); // даже при включённом антидребезге
        assert_eq!(
            run(&p, &[(0, i(1), Good), (100, None, Bad), (200, None, Bad), (300, i(1), Good)]),
            [true, true, false, true]
        );
    }

    #[test]
    fn bad_frame_with_null_value_repeats_only_on_full_resend() {
        let p = params(0.0, 0.0, 0, 30_000);
        assert_eq!(run(&p, &[(0, None, Bad), (2000, None, Bad), (30_000, None, Bad)]), [true, false, true]);
    }

    #[test]
    fn min_interval_blocks_a_change_but_does_not_lose_it() {
        let p = params(0.0, 0.0, 5000, 30_000);
        // 2 с: изменение внутри окна — не публикуется; 4 с: всё ещё внутри; 6 с: окно прошло, значение
        // так и не вернулось — уходит первым же опросом.
        assert_eq!(
            run(&p, &[(0, f(1.0), Good), (2000, f(2.0), Good), (4000, f(2.0), Good), (6000, f(2.0), Good)]),
            [true, false, false, true]
        );
        // Вернулось к опубликованному за окно — публиковать нечего.
        assert_eq!(
            run(&p, &[(0, f(1.0), Good), (2000, f(2.0), Good), (4000, f(1.0), Good), (6000, f(1.0), Good)]),
            [true, false, false, false]
        );
    }

    #[test]
    fn full_resend_wins_over_min_interval() {
        // Правило 3 проверяется раньше правила 4.
        let p = params(0.0, 0.0, 60_000, 30_000);
        assert_eq!(run(&p, &[(0, i(1), Good), (30_000, i(1), Good)]), [true, true]);
    }

    #[test]
    fn absolute_deadband_compares_with_last_published_so_drift_accumulates() {
        let p = params(0.5, 0.0, 0, 0);
        // Публикуется 10.0; 10.3 и 10.4 — внутри зоны (сравнение с 10.0, а не с прошлым опросом);
        // 10.6 — вышло за зону; следующий отсчёт — от 10.6.
        assert_eq!(
            run(
                &p,
                &[(0, f(10.0), Good), (1, f(10.3), Good), (2, f(10.4), Good), (3, f(10.6), Good), (4, f(10.9), Good)]
            ),
            [true, false, false, true, false]
        );
    }

    #[test]
    fn zone_boundary_is_exclusive() {
        let p = params(0.5, 0.0, 0, 0);
        assert_eq!(run(&p, &[(0, f(10.0), Good), (1, f(10.5), Good), (2, f(10.5001), Good)]), [true, false, true]);
    }

    #[test]
    fn percent_deadband_scales_with_published_value_and_max_of_both_wins() {
        let p = params(0.0, 10.0, 0, 0); // 10 % от опубликованного
        assert_eq!(run(&p, &[(0, f(100.0), Good), (1, f(109.0), Good), (2, f(111.0), Good)]), [true, false, true]);
        let both = params(5.0, 1.0, 0, 0); // max(5, 1 % от 100 = 1) = 5
        assert_eq!(run(&both, &[(0, f(100.0), Good), (1, f(104.0), Good), (2, f(106.0), Good)]), [true, false, true]);
    }

    #[test]
    fn zero_zone_means_any_difference() {
        let p = params(0.0, 0.0, 0, 0);
        assert_eq!(run(&p, &[(0, f(1.0), Good), (1, f(1.0), Good), (2, f(1.0000001), Good)]), [true, false, true]);
    }

    #[test]
    fn integers_and_floats_are_compared_numerically() {
        let p = params(0.0, 0.0, 0, 0);
        let (base, mut last) = (T0(), None);
        let d = |v: TagValue, ms: u64, last: &mut Option<Last>| {
            decide(&p, last, Some(&v), Good, base + Duration::from_millis(ms))
        };
        assert!(d(TagValue::Int(3), 0, &mut last));
        assert!(!d(TagValue::F64(3.0), 1, &mut last), "3 и 3.0 — одно значение");
        assert!(!d(TagValue::F32(3.0), 2, &mut last));
        assert!(d(TagValue::F32(3.5), 3, &mut last));
    }

    #[test]
    fn non_numbers_are_compared_for_equality_and_deadband_does_not_apply() {
        let p = params(100.0, 50.0, 0, 0);
        let s = |t: &str| Some(TagValue::Text(t.into()));
        let b = |v: bool| Some(TagValue::Bool(v));
        assert_eq!(run(&p, &[(0, s("REC1"), Good), (1, s("REC1"), Good), (2, s("REC2"), Good)]), [true, false, true]);
        assert_eq!(run(&p, &[(0, b(false), Good), (1, b(false), Good), (2, b(true), Good)]), [true, false, true]);
        // Строка → число — другое значение, зона не применяется.
        assert_eq!(run(&p, &[(0, s("1"), Good), (1, i(1), Good)]), [true, true]);
    }

    #[test]
    fn nan_equals_nan() {
        let p = params(0.0, 0.0, 0, 0);
        assert_eq!(
            run(&p, &[(0, f(f64::NAN), Good), (1, f(f64::NAN), Good), (2, f(1.0), Good), (3, f(f64::NAN), Good)]),
            [true, false, true, true]
        );
    }

    #[test]
    fn unpublished_decision_is_not_remembered() {
        // Вызывающий не зовёт decide для кадров, которые не уйдут (BAD при выключенных BAD-кадрах):
        // состояние остаётся прежним, и следующее значение сравнивается с реально опубликованным.
        let p = params(0.0, 0.0, 0, 30_000);
        let (base, mut last) = (T0(), None);
        assert!(decide(&p, &mut last, Some(&TagValue::Int(1)), Good, base));
        // здесь пропущен decide(None, Bad)
        assert!(!decide(&p, &mut last, Some(&TagValue::Int(1)), Good, base + Duration::from_secs(5)));
    }

    #[test]
    fn history_params_take_tag_overrides_over_defaults() {
        let s = HistorySettings {
            deadband: 0.0,
            deadband_percent: 0.0,
            min_interval: Duration::ZERO,
            max_interval: Duration::from_secs(600),
        };
        assert_eq!(FilterParams::history(&s, &HistoryConfig::default()), params(0.0, 0.0, 0, 600_000));
        let tag = HistoryConfig {
            deadband: Some(0.5),
            deadband_percent: None,
            min_interval_ms: Some(1000),
            max_interval_ms: Some(0), // «пульс» тегу выключен
        };
        assert_eq!(FilterParams::history(&s, &tag), params(0.5, 0.0, 1000, 0));
    }
}
