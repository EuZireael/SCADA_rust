//! Песочница Lua для снимка, который присылает «прошивка».
//!
//! Снимок присылает контроллер — то есть любой, кто ответит на порту PAC, поэтому стейт — только
//! чистые вычисления: без io/os/package, файлов, модулей и загрузки кода, с потолком памяти и
//! времени на вызов. Это настоящий Lua 5.1 (как в самом ptusa). Копия `src/sandbox.rs` шлюза.

use std::time::{Duration, Instant};

use anyhow::Result;
use mlua::{HookTriggers, Lua, LuaOptions, StdLib, Table, Value, VmState};

/// Как часто проверять время (в инструкциях VM).
const HOOK_EVERY: u32 = 10_000;

/// Глобальные функции базовой библиотеки, убираемые из стейта: байткод (`load`, `loadstring`,
/// `string.dump` — Lua 5.1 его не проверяет), файлы и модули, `pcall`/`xpcall`/`coroutine` (перехват
/// ошибки лимита времени), управление GC и окружениями. Снимок ptusa — только присваивания таблиц.
pub const REMOVED_GLOBALS: &[&str] = &[
    "load",
    "loadstring",
    "dofile",
    "loadfile",
    "require",
    "module",
    "pcall",
    "xpcall",
    "coroutine",
    "collectgarbage",
    "getfenv",
    "setfenv",
    "newproxy",
];

/// Новый стейт: base/string/table/math без опасного, потолок памяти `memory` байт.
pub fn new_lua(memory: usize) -> Result<Lua> {
    let lua = Lua::new_with(StdLib::TABLE | StdLib::STRING | StdLib::MATH, LuaOptions::default())?;
    let globals = lua.globals();
    for &name in REMOVED_GLOBALS {
        globals.set(name, Value::Nil)?;
    }
    globals.get::<Table>("string")?.set("dump", Value::Nil)?;
    lua.set_memory_limit(memory)?;
    Ok(lua)
}

/// Выполнить `f` не дольше `budget`: перехват по счётчику инструкций обрывает бесконечный цикл
/// ошибкой, которую скрипт не поймает. «Успешный» выход после превышения срока — тоже ошибка.
pub fn run_limited<R>(lua: &Lua, budget: Duration, f: impl FnOnce() -> mlua::Result<R>) -> mlua::Result<R> {
    let started = Instant::now();
    let deadline = started + budget;
    lua.set_hook(HookTriggers::new().every_nth_instruction(HOOK_EVERY), move |_, _| {
        if Instant::now() > deadline {
            Err(mlua::Error::runtime(format!("исполняется дольше лимита {} мс", budget.as_millis())))
        } else {
            Ok(VmState::Continue)
        }
    })?;
    let result = f();
    lua.remove_hook();
    match result {
        Ok(_) if started.elapsed() > budget => {
            Err(mlua::Error::runtime(format!("исполняется дольше лимита {} мс", budget.as_millis())))
        }
        other => other,
    }
}

/// Для тестов, которые без защиты зависли бы навсегда: выполнить `f` в отдельном потоке и дождаться
/// не дольше `secs`.
#[cfg(test)]
pub(crate) fn within<T: Send + 'static>(secs: u64, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(secs))
        .unwrap_or_else(|_| panic!("не уложились в {secs} с: код вышел из-под лимита времени"))
}
