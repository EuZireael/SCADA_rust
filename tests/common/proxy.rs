//! TCP-прокси для тестов обрыва связи: встаёт между шлюзом и симулятором и по команде
//! пропускает трафик, рвёт соединения или «вешает» их.

use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Трафик идёт насквозь.
    Pass,
    /// Соединения закрываются, новые принимаются и сразу закрываются (кабель выдернут
    /// у ПЛК, ПЛК перезагружается).
    Reset,
    /// Соединения остаются открытыми, но ответов нет (обрыв где-то посередине сети, ПЛК
    /// завис) — проверка таймаутов.
    Hang,
}

pub struct Proxy {
    pub port: u16,
    mode: watch::Sender<Mode>,
    accept: JoinHandle<()>,
}

impl Proxy {
    /// Прокси на свободном порту 127.0.0.1 к `upstream` (`host:port`).
    pub async fn start(upstream: String) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (mode, rx) = watch::channel(Mode::Pass);
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                tokio::spawn(serve(client, upstream.clone(), rx.clone()));
            }
        });
        Proxy { port, mode, accept }
    }

    /// Переключить режим прокси: пропускать, рвать соединения или молчать.
    pub fn set(&self, mode: Mode) {
        self.mode.send_replace(mode);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// Одно соединение. Любая смена режима его закрывает (после Hang шлюз должен заметить
/// обрыв и переподключиться), кроме перехода Pass → Hang: тогда связь с upstream рвётся,
/// а сокет шлюза остаётся открытым и молчит до следующей смены режима.
async fn serve(mut client: TcpStream, upstream: String, mut mode: watch::Receiver<Mode>) {
    let current = *mode.borrow_and_update();
    match current {
        Mode::Reset => {}
        Mode::Hang => {
            let _ = mode.changed().await;
        }
        Mode::Pass => {
            let Ok(mut server) = TcpStream::connect(&upstream).await else { return };
            tokio::select! {
                _ = copy_bidirectional(&mut client, &mut server) => {}
                _ = mode.changed() => {
                    if *mode.borrow() == Mode::Hang {
                        drop(server);
                        let _ = mode.changed().await;
                    }
                }
            }
        }
    }
}
