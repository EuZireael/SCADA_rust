"""
Тесты форматирования значения поля прибора (sim_records.fmt_value).

Формат должен совпадать с тем, как значения пишет реальный ptusa в строке
device::save_device: bool → 0/1, целое float → без дробной части, дробное → 2 знака.
Шлюз парсит эти строки, поэтому формат — часть контракта.
"""
import pytest

from sim_records import fmt_value


@pytest.mark.parametrize("raw,dtype,expected", [
    (None, "float", "0"),       # нет значения → "0"
    (None, "bool", "0"),
    (0, "bool", "0"),
    (1, "bool", "1"),
    (2.5, "bool", "1"),         # любое ненулевое → "1"
    (0.0, "bool", "0"),
    (3.0, "float", "3"),        # целое float → без дробной части
    (10, "int", "10"),
    (3.14159, "float", "3.14"), # дробное → 2 знака
    (-2.5, "float", "-2.50"),
    (-3.0, "float", "-3"),
])
def test_fmt_value(raw, dtype, expected):
    assert fmt_value(raw, dtype) == expected
