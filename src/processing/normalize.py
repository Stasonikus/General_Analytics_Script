# src/normalize.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


T1_BLOCK_START = "Аналитика по пунктам пропуска"
T1_BLOCK_END = "Статистика пломб по типам перевозки (Казахстан)"


def _clean_quotes(s: str, quotes: List[str]) -> str:
    for q in quotes:
        s = s.replace(q, "")
    return s.strip()


def _normalize_spaces_and_dashes(s: str) -> str:
    # приводим разные дефисы к обычному "-", и схлопываем лишние пробелы
    s = s.replace("–", "-").replace("—", "-")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()


def _title_ru_simple(s: str) -> str:
    """
    Простейшее правило: первая буква заглавная, остальные строчные.
    Пример: "АЛАКОЛЬ" -> "Алаколь", "Б. КОНЫСБАЕВА" -> "Б. конысбаева"
    """
    s = s.strip()
    if not s:
        return s
    return s[:1].upper() + s[1:].lower()


def _normalize_pp_name(raw_after_marker: str, normalization: Dict[str, Any]) -> str:
    quotes = normalization.get("quotes_to_strip", ['"', "«", "»"])
    alias_map: Dict[str, str] = normalization.get("pp_aliases", {}) or {}

    s = _clean_quotes(raw_after_marker, quotes)
    s = _normalize_spaces_and_dashes(s)
    s = _title_ru_simple(s)

    # применяем алиас уже ПОСЛЕ базовой нормализации
    # (ключи алиасов ожидаем в том же нормализованном виде)
    s = alias_map.get(s, s)
    return s


def _norm_for_header_match(val: str) -> str:
    # нормализация строки для сравнения заголовков
    return _normalize_spaces_and_dashes(val).strip().lower()


def find_t1_block_range(ws) -> Tuple[int, int]:
    """
    Находит границы блока Т1 на листе «Отчёт»:
    [Аналитика по пунктам пропуска] ... [Статистика пломб по типам перевозки (Казахстан)]

    Возвращает (start_row, end_row) — номера строк, где стоят заголовки.
    Если не нашли — возвращаем (1, ws.max_row + 1) как “весь лист”.
    """
    start_row = None
    end_row = None

    start_key = _norm_for_header_match(T1_BLOCK_START)
    end_key = _norm_for_header_match(T1_BLOCK_END)

    for row_idx in range(1, ws.max_row + 1):
        cell = ws.cell(row=row_idx, column=1).value
        if not isinstance(cell, str):
            continue

        key = _norm_for_header_match(cell)
        if start_row is None and key == start_key:
            start_row = row_idx
            continue

        if start_row is not None and key == end_key:
            end_row = row_idx
            break

    if start_row is None or end_row is None or end_row <= start_row:
        return 1, ws.max_row + 1

    return start_row, end_row


def extract_pp_names(
    ws,
    normalization: Dict[str, Any],
    allowed_pp: Optional[List[str]] = None,
    row_range: Optional[Tuple[int, int]] = None,
) -> Tuple[List[str], List[str]]:
    """
    Проходит по листу «Отчёт», ищет строки с маркерами ПП (т/п|тп|таможенный пост),
    извлекает название после маркера, нормализует.
    Если задан allowed_pp, то возвращает только ПП из этого списка.

    row_range: (start_row, end_row) — диапазон строк [start_row; end_row),
              если не задан, идём по всему листу.

    Возвращает:
      - found_pp: уникальные ПП (в порядке появления)
      - ignored_pp: уникальные ПП, которые были найдены, но отфильтрованы (не входят в allowed_pp)
    """
    markers = [
        m.lower()
        for m in normalization.get("pp_markers", ["т/п", "тп", "таможенный пост"])
    ]

    allowed_set = set(allowed_pp or [])
    use_filter = allowed_pp is not None and len(allowed_set) > 0

    found: List[str] = []
    ignored: List[str] = []
    seen_found = set()
    seen_ignored = set()

    if row_range is None:
        start_i, end_i = 1, ws.max_row + 1
    else:
        start_i, end_i = row_range

    # предполагаем, что текст ПП в колонке A (1)
    for row_idx in range(start_i, end_i):
        cell = ws.cell(row=row_idx, column=1).value
        if not isinstance(cell, str):
            continue

        raw = cell.strip()
        low = raw.lower()

        # ищем маркер в строке (маркер может быть не в начале)
        hit_pos = -1
        hit_len = 0
        for m in markers:
            pos = low.find(m)
            if pos != -1:
                hit_pos = pos
                hit_len = len(m)
                break
        if hit_pos == -1:
            continue

        # берём всё после маркера
        after = raw[hit_pos + hit_len :].strip()
        if not after:
            continue

        name = _normalize_pp_name(after, normalization)
        if not name:
            continue

        if use_filter and name not in allowed_set:
            if name not in seen_ignored:
                seen_ignored.add(name)
                ignored.append(name)
            continue

        if name not in seen_found:
            seen_found.add(name)
            found.append(name)

    return found, ignored


def normalize_report(
    report_1_raw: Any,
    normalization: Dict[str, Any],
    allowed_pp: Optional[List[str]] = None,
) -> Any:
    """
    Текущая нормализация (этап для Т1):
      - находим диапазон блока Т1:
        от «Аналитика по пунктам пропуска» до «Статистика пломб по типам перевозки (Казахстан)»
      - извлекаем и нормализуем названия ПП только внутри этого диапазона
      - (опционально) фильтруем только ПП из allowed_pp (TP_ZHD_LIST_T1)
    """
    ws = report_1_raw["ws"]

    start_row, end_row = find_t1_block_range(ws)
    # записываем в контекст — удобно видеть, что диапазон найден
    report_1_raw["t1_block_range"] = (start_row, end_row)

    # Мы берём строки МЕЖДУ заголовками (то есть исключаем строки с заголовками)
    scan_range = (start_row + 1, end_row)

    pp_names, pp_ignored = extract_pp_names(
        ws,
        normalization,
        allowed_pp=allowed_pp,
        row_range=scan_range,
    )

    report_1_raw["pp_names_found"] = pp_names
    report_1_raw["pp_names_ignored"] = pp_ignored
    return report_1_raw
