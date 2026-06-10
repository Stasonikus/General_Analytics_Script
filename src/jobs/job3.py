from datetime import datetime

import pandas as pd

from src.config import OUT_DIR, IN_DIR
from src.io_excel import read_excel, write_excel_sheets, get_single_input_file
from src.transform import basic_cleanup, protect_from_excel_formulas
from src.logger import get_logger


logger = get_logger(__name__)


INPUT_PATTERNS = [
    "Перевозки_НП_влад*.xlsx",
    "Перевозки_НП_влад*.xls",
]

NUMBER_FILE_NAME = "number.xlsx"
NUMBER_SHEET_NAME = "Arrow"

OUTPUT_FILE_BASE_NAME = "result_job3"

OUTPUT_SHEET_RESULT_NAME = "лист2"
OUTPUT_SHEET_SUMMARY_NAME = "лист3"

USE_OWNER_FILTER = True
USE_STATUS_FILTER = True
USE_COMPANY_FILTER = True
USE_DATE_FILTER = True

COLUMN_RECORD_OWNER = "Владелец записи"
COLUMN_STATUS = "Статус"
COLUMN_STATUS_DATE = "Status date"
COLUMN_SEAL_PLAN = "План установки пломб"
COLUMN_COMPANY_ID = "company_id"

ALLOWED_STATUSES = {"activated", "finished"}
ALLOWED_RECORD_OWNERS = {
    "КЕДЕН KEDEN",
    "ТОО «Институт космической техники и технологий»",
    "ЭСФ ESF ТТН TTN КГД Интеграция",
}

DATE_FROM = pd.Timestamp("2026-02-11")

OWNER_ARROWTECH = "Arrowtech"
OWNER_IKTT = "ИКТТ"

MONTH_NAMES_RU = {
    2: "Февраль",
    3: "Март",
    4: "Апрель",
}


def build_output_file_name(base_name: str) -> str:
    return f"{base_name}_{datetime.now().strftime('%Y%m%d')}.xlsx"


def normalize_number(value) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip()

    if text.lower() in {"nan", "none"}:
        return ""

    if text.endswith(".0"):
        text = text[:-2]

    return text.strip()


def load_number_set() -> set[str]:
    number_file = IN_DIR / NUMBER_FILE_NAME

    if not number_file.exists():
        raise FileNotFoundError(f"Не найден файл {NUMBER_FILE_NAME}")

    logger.info(f"Файл шаблона Number: {number_file.name}")
    logger.info(f"Чтение листа шаблона: {NUMBER_SHEET_NAME}")

    df = read_excel(number_file, sheet_name=NUMBER_SHEET_NAME)

    if df.empty:
        logger.warning(f"Лист {NUMBER_SHEET_NAME} в файле {NUMBER_FILE_NAME} пустой")
        return set()

    col = df.columns[0]
    numbers = df[col].apply(normalize_number)
    numbers = numbers[numbers != ""]

    result = set(numbers.tolist())

    logger.info(f"Загружено номеров Arrowtech: {len(result)}")
    return result


def split_seals(value) -> list[str]:
    if pd.isna(value):
        return []

    text = str(value).replace("\n", ",").replace(";", ",")
    parts = [v.strip() for v in text.split(",")]

    return [normalize_number(v) for v in parts if v.strip()]


def expand_seals(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, row in df.iterrows():
        seals = split_seals(row[COLUMN_SEAL_PLAN])

        for seal in seals:
            new_row = row.copy()
            new_row[COLUMN_SEAL_PLAN] = seal
            rows.append(new_row)

    if not rows:
        result = pd.DataFrame(columns=df.columns)
    else:
        result = pd.DataFrame(rows)

    logger.info(f"После развертывания НП строк: {len(result)}")
    return result


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["m"] = work["Дата"].dt.month

    rows = []
    for m in [2, 3, 4]:
        rows.append({
            "": MONTH_NAMES_RU[m],
            "ИКТТ": len(work[(work["m"] == m) & (work["Владелец"] == OWNER_IKTT)]),
            "Arrowtech": len(work[(work["m"] == m) & (work["Владелец"] == OWNER_ARROWTECH)]),
            "Всего": len(work[work["m"] == m]),
        })

    summary = pd.DataFrame(rows)

    # 👉 добавляем строку "Итого"
    total_row = {
        "": "Итого, всего:",
        "ИКТТ": summary["ИКТТ"].sum(),
        "Arrowtech": summary["Arrowtech"].sum(),
        "Всего": summary["Всего"].sum(),
    }

    summary = pd.concat([summary, pd.DataFrame([total_row])], ignore_index=True)

    return summary


def count_matches_after_expand(df: pd.DataFrame, number_set: set[str]) -> tuple[int, int]:
    expanded = expand_seals(df)

    if expanded.empty:
        return 0, 0

    match_count = expanded[COLUMN_SEAL_PLAN].apply(
        lambda x: normalize_number(x) in number_set
    ).sum()

    return len(expanded), int(match_count)


def log_match_diagnostics(df_prepared: pd.DataFrame, number_set: set[str]) -> None:
    logger.info("===== ДИАГНОСТИКА ПО СОВПАДЕНИЯМ С NUMBER =====")

    expanded_count, match_count = count_matches_after_expand(df_prepared, number_set)
    logger.info(
        f"[ДИАГ] 0. Без фильтров: "
        f"до={len(df_prepared)}, после expand={expanded_count}, совпадения={match_count}"
    )

    df_step = df_prepared.copy()

    if USE_OWNER_FILTER:
        df_step = df_step[df_step[COLUMN_RECORD_OWNER].isin(ALLOWED_RECORD_OWNERS)]
        expanded_count, match_count = count_matches_after_expand(df_step, number_set)
        logger.info(
            f"[ДИАГ] 1. После фильтра владельца записи: "
            f"до={len(df_step)}, после expand={expanded_count}, совпадения={match_count}"
        )
    else:
        logger.info("[ДИАГ] 1. Фильтр владельца записи ОТКЛЮЧЕН")

    if USE_STATUS_FILTER:
        df_step = df_step[df_step[COLUMN_STATUS].isin(ALLOWED_STATUSES)]
        expanded_count, match_count = count_matches_after_expand(df_step, number_set)
        logger.info(
            f"[ДИАГ] 2. После фильтра статуса: "
            f"до={len(df_step)}, после expand={expanded_count}, совпадения={match_count}"
        )
    else:
        logger.info("[ДИАГ] 2. Фильтр статуса ОТКЛЮЧЕН")

    if USE_COMPANY_FILTER:
        df_step = df_step[df_step[COLUMN_COMPANY_ID] != "1"]
        expanded_count, match_count = count_matches_after_expand(df_step, number_set)
        logger.info(
            f"[ДИАГ] 3. После удаления company_id=1: "
            f"до={len(df_step)}, после expand={expanded_count}, совпадения={match_count}"
        )
    else:
        logger.info("[ДИАГ] 3. Фильтр company_id ОТКЛЮЧЕН")

    expanded_df = expand_seals(df_step)

    if USE_DATE_FILTER:
        expanded_df = expanded_df[expanded_df["Дата_статуса"].notna()]
        expanded_df = expanded_df[expanded_df["Дата_статуса"] >= DATE_FROM]
        match_count = expanded_df[COLUMN_SEAL_PLAN].apply(
            lambda x: normalize_number(x) in number_set
        ).sum()
        logger.info(
            f"[ДИАГ] 4. После фильтра даты >= {DATE_FROM.strftime('%d.%m.%Y')}: "
            f"строк={len(expanded_df)}, совпадения={int(match_count)}"
        )
    else:
        match_count = expanded_df[COLUMN_SEAL_PLAN].apply(
            lambda x: normalize_number(x) in number_set
        ).sum()
        logger.info(
            f"[ДИАГ] 4. Фильтр даты ОТКЛЮЧЕН: "
            f"строк={len(expanded_df)}, совпадения={int(match_count)}"
        )

    logger.info("===== КОНЕЦ ДИАГНОСТИКИ =====")


def run():
    input_file = get_single_input_file(INPUT_PATTERNS)
    number_set = load_number_set()

    logger.info(f"Файл обработки: {input_file.name}")

    df_raw = read_excel(input_file, header=14)

    df = basic_cleanup(df_raw)
    df = protect_from_excel_formulas(df)

    df[COLUMN_STATUS] = df[COLUMN_STATUS].astype(str).str.lower().str.strip()
    df[COLUMN_RECORD_OWNER] = df[COLUMN_RECORD_OWNER].astype(str).str.strip()
    df[COLUMN_SEAL_PLAN] = df[COLUMN_SEAL_PLAN].astype(str).str.strip()
    df[COLUMN_COMPANY_ID] = df[COLUMN_COMPANY_ID].astype(str).str.strip()

    # отдельная служебная колонка, чтобы не конфликтовать с исходной колонкой "Дата"
    df["Дата_статуса"] = pd.to_datetime(df[COLUMN_STATUS_DATE], errors="coerce")

    log_match_diagnostics(df, number_set)

    if USE_OWNER_FILTER:
        df = df[df[COLUMN_RECORD_OWNER].isin(ALLOWED_RECORD_OWNERS)]
        logger.info(f"После фильтра владельца записи: {len(df)} строк")
    else:
        logger.info("Фильтр владельца записи ОТКЛЮЧЕН")

    if USE_STATUS_FILTER:
        df = df[df[COLUMN_STATUS].isin(ALLOWED_STATUSES)]
        logger.info(f"После фильтра статуса: {len(df)} строк")
    else:
        logger.info("Фильтр статуса ОТКЛЮЧЕН")

    if USE_COMPANY_FILTER:
        df = df[df[COLUMN_COMPANY_ID] != "1"]
        logger.info(f"После удаления company_id=1: {len(df)} строк")
    else:
        logger.info("Фильтр company_id ОТКЛЮЧЕН")

    df = expand_seals(df)

    if USE_DATE_FILTER:
        df = df[df["Дата_статуса"].notna()]
        df = df[df["Дата_статуса"] >= DATE_FROM]
        logger.info(f"После фильтра даты >= {DATE_FROM.strftime('%d.%m.%Y')}: {len(df)} строк")
    else:
        logger.info("Фильтр даты ОТКЛЮЧЕН")

    df["Владелец"] = df[COLUMN_SEAL_PLAN].apply(
        lambda x: OWNER_ARROWTECH if normalize_number(x) in number_set else OWNER_IKTT
    )

    result_df = pd.DataFrame({
        "Дата": df["Дата_статуса"],
        "Серийный номер": df[COLUMN_SEAL_PLAN].apply(normalize_number),
        "Владелец": df["Владелец"],
    })

    result_df = result_df.sort_values("Дата").reset_index(drop=True)
    logger.info(f"Лист 2. Итоговых строк: {len(result_df)}")

    summary_df = build_summary(result_df)
    logger.info(f"Лист 3. Строк в сводной таблице: {len(summary_df)}")

    output_file = OUT_DIR / build_output_file_name(OUTPUT_FILE_BASE_NAME)

    write_excel_sheets(
        {
            OUTPUT_SHEET_RESULT_NAME: result_df,
            OUTPUT_SHEET_SUMMARY_NAME: summary_df,
        },
        output_file,
    )

    logger.info(f"Готово: {output_file}")