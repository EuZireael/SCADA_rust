//! Надзор за задачами шлюза.
//!
//! Задача опроса или приёма команд, упавшая с паникой (ошибка в скрипте, отравленный мьютекс, баг),
//! без надзора молча исчезла бы: процесс «жив», `/actuator/health` отвечает, а контроллер больше не
//! опрашивается или команды не принимаются. Надзиратель перезапускает такую задачу (пауза 1 → 30 с, при
//! долгой работе отсчёт паузы начинается заново), пишет в журнал, считает перезапуски
//! (`scada_task_restarts_total{task}`) и кладёт событие SYSTEM/ERROR — монитор и оператор видят сбой.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::events::{Event, EventSink};
use crate::metrics::Metrics;

/// Пауза перед первым перезапуском задачи.
const MIN_BACKOFF: Duration = Duration::from_secs(1);
/// Потолок паузы: она удваивается до этого значения.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Задача проработала столько — прежние сбои забыты, пауза снова минимальная.
const HEALTHY_AFTER: Duration = Duration::from_secs(60);

/// Запустить задачу под надзором. `make` создаёт новый экземпляр задачи при каждом (пере)запуске;
/// задача должна работать до `cancel` — выход раньше считается сбоем.
pub fn supervise<F, Fut>(
    name: &str,
    cancel: CancellationToken,
    metrics: Arc<Metrics>,
    events: EventSink,
    make: F,
) -> JoinHandle<()>
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let name = name.to_string();
    tokio::spawn(async move {
        let mut backoff = MIN_BACKOFF;
        loop {
            let started = Instant::now();
            let outcome = tokio::spawn(make()).await;
            if cancel.is_cancelled() {
                return;
            }
            let why = match outcome {
                Ok(()) => "завершилась, хотя остановки не было".to_string(),
                Err(e) if e.is_panic() => format!("паника: {}", panic_message(e.into_panic())),
                Err(e) => format!("отменена: {e}"),
            };
            if started.elapsed() >= HEALTHY_AFTER {
                backoff = MIN_BACKOFF;
            }
            metrics.task_restarts.with_label_values(&[&name]).inc();
            error!("Задача «{name}»: {why}. Перезапуск через {} с", backoff.as_secs());
            events.emit(Event::new(
                "SYSTEM",
                "Gateway",
                "ERROR",
                format!("Задача «{name}» упала ({why}); перезапуск через {} с", backoff.as_secs()),
            ));
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    })
}

/// Текст паники из её полезной нагрузки (`String`, `&str` или «без сообщения»).
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(s) => *s,
        Err(payload) => {
            payload.downcast_ref::<&str>().map_or_else(|| "без сообщения".to_string(), |s| (*s).to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::events;

    fn parts() -> (Arc<Metrics>, EventSink, tokio::sync::mpsc::Receiver<Event>) {
        let metrics = Arc::new(Metrics::new());
        let (sink, rx) = events::channel(metrics.clone());
        (metrics, sink, rx)
    }

    /// Задача падает дважды, на третьем запуске работает: два перезапуска, паузы 1 и 2 с, два события.
    #[tokio::test(start_paused = true)]
    async fn panicking_task_is_restarted_with_growing_pauses() {
        let (metrics, sink, mut rx) = parts();
        let cancel = CancellationToken::new();
        let runs = Arc::new(AtomicU32::new(0));
        let (r, c) = (runs.clone(), cancel.clone());
        let handle = supervise("опрос", cancel.clone(), metrics.clone(), sink, move || {
            let (r, c) = (r.clone(), c.clone());
            async move {
                if r.fetch_add(1, Ordering::SeqCst) < 2 {
                    panic!("сломалось");
                }
                c.cancelled().await;
            }
        });
        let started = tokio::time::Instant::now();
        while runs.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(started.elapsed().as_secs(), 3, "пауза 1 с, затем 2 с");
        assert_eq!(metrics.task_restarts.with_label_values(&["опрос"]).get(), 2);
        let first = rx.recv().await.expect("событие о падении");
        assert!(first.message.contains("опрос") && first.message.contains("сломалось"), "{}", first.message);
        assert_eq!((first.event_type.as_str(), first.severity.as_str()), ("SYSTEM", "ERROR"));
        cancel.cancel();
        handle.await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn task_that_returns_early_is_restarted_but_a_stop_ends_supervision() {
        let (metrics, sink, _rx) = parts();
        let cancel = CancellationToken::new();
        let runs = Arc::new(AtomicU32::new(0));
        let r = runs.clone();
        let handle = supervise("команды", cancel.clone(), metrics.clone(), sink, move || {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst); // выход без остановки — сбой
            }
        });
        while runs.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        cancel.cancel();
        handle.await.unwrap();
        assert!(metrics.task_restarts.with_label_values(&["команды"]).get() >= 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_task_that_stops_on_cancel_is_not_counted_as_a_failure() {
        let (metrics, sink, mut rx) = parts();
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        let handle = supervise("тихая", cancel.clone(), metrics.clone(), sink, move || {
            let c = c.clone();
            async move { c.cancelled().await }
        });
        tokio::time::sleep(Duration::from_secs(5)).await;
        cancel.cancel();
        handle.await.unwrap();
        assert_eq!(metrics.task_restarts.with_label_values(&["тихая"]).get(), 0);
        assert!(rx.try_recv().is_err(), "штатная остановка — не событие");
    }

    #[test]
    fn panic_messages_are_extracted() {
        assert_eq!(panic_message(Box::new("строка")), "строка");
        assert_eq!(panic_message(Box::new(String::from("владеемая"))), "владеемая");
        assert_eq!(panic_message(Box::new(42)), "без сообщения");
    }
}
