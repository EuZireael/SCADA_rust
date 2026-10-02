//! Песочница Lua для чужого кода в процессе шлюза: ответы PAC-контроллера и пользовательские скрипты.
//!
//! Код присылает контроллер (то есть любой, кто ответит на порту PAC) или кладёт инженер в папку
//! скриптов, поэтому стейт — только чистые вычисления: без io/os/package, файлов, модулей и загрузки
//! кода, с потолком памяти и временем на вызов. Это настоящий Lua 5.1 (как в самом ptusa).

use std::time::{Duration, Instant};

use anyhow::Result;
use mlua::{HookTriggers, Lua, LuaOptions, StdLib, Table, Value, VmState};

/// Как часто проверять время (в инструкциях VM).
const HOOK_EVERY: u32 = 10_000;

/// Глобальные функции базовой библиотеки, убираемые из стейта:
/// * `load`, `loadstring`, `string.dump` — байткод: Lua 5.1 его не проверяет, и собранный
///   скриптом байткод — выход из песочницы в память процесса;
/// * `dofile`, `loadfile`, `require`, `module` — файлы и модули;
/// * `pcall`, `xpcall`, `coroutine` — перехват ошибки лимита времени и продолжение цикла (в Lua 5.1
///   `coroutine` входит в базовую библиотеку и грузится всегда: `coroutine.resume` ловил бы ошибку);
/// * `collectgarbage`, `getfenv`, `setfenv`, `newproxy` — управление GC и окружениями.
///
/// Снимок ptusa — только присваивания таблиц (`t.X = t.X or {}` и литералы), скриптам обработки
/// значений — арифметика и условия: ничего из этого им не нужно.
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
/// ошибкой, которую скрипт не поймает (`pcall` убран, `coroutine` нет). Если `f` вернулся «успешно»,
/// но уложился не в срок (ошибку проглотил, например, `string.gsub` с колбэком), это тоже превышение.
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

/// Для тестов, которые без защиты зависли бы навсегда: выполнить `f` в отдельном потоке и дождаться не
/// дольше `secs`. Регрессия (нет лимита времени) — падение теста, а не зависшая сборка.
#[cfg(test)]
pub(crate) fn within<T: Send + 'static>(secs: u64, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(secs))
        .unwrap_or_else(|_| panic!("не уложились в {secs} с: код вышел из-под лимита времени"))
}
