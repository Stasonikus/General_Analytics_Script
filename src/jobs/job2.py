from datetime import datetime

import pandas as pd

from src.config import OUT_DIR
from src.io_excel import (
    get_single_input_file,
    read_excel,
    read_table,
    write_excel_sheets,
)
from src.transform import basic_cleanup, protect_from_excel_formulas
from src.logger import get_logger


logger = get_logger(__name__)


INPUT_PATTERNS = [
    "НП_по_владельцам*.xlsx",
    "НП_по_владельцам*.xls",
    "НП_по_владельцам*.csv",
]

OUTPUT_FILE_BASE_NAME = "result_job2"
SOURCE_COLUMN_NAME = "source_file"

INPUT_SHEET_NAME = 0
OUTPUT_SHEET_RAW_NAME = "лист1"
OUTPUT_SHEET_ARROWTECH_NAME = "лист2"
OUTPUT_SHEET_IKTT_NAME = "лист3"
OUTPUT_SHEET_SUMMARY_NAME = "лист4"
OUTPUT_SHEET_PERIOD_NAME = "лист5"

COLUMN_NUMBER = "Number"
COLUMN_ISSUE_DATE = "Дата выдачи"
COLUMN_SEAL = "Пломба"
COLUMN_OWNER = "Владелец пломбы"

OWNER_ARROWTECH = 'ТОО "Arrowtech" (Арроутек)'
OWNER_IKTT = "ТОО «Институт космической техники и технологий»"

DATE_FROM = pd.Timestamp("2026-02-11 00:00:00")
DATE_TO = pd.Timestamp("2026-04-06 23:59:59")

PERIOD_FROM = pd.Timestamp("2026-02-11 00:00:00")
PERIOD_TO = pd.Timestamp("2026-03-11 23:59:59")

MONTH_NAMES_RU = {
    2: "Февраль",
    3: "Март",
    4: "Апрель",
}


def build_output_file_name(base_name: str) -> str:
    current_date = datetime.now().strftime("%Y%m%d")
    return f"{base_name}_{current_date}.xlsx"


def validate_input_columns(df: pd.DataFrame) -> None:
    required_columns = {
        COLUMN_NUMBER,
        COLUMN_ISSUE_DATE,
        COLUMN_SEAL,
        COLUMN_OWNER,
    }
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise KeyError(
            f"В исходной таблице отсутствуют обязательные колонки: {missing}"
        )


def extract_status_from_number(value) -> str:
    """
    Из строки вида:
    - 'KZ_3137 finished'
    - '3acf4346-f546-4b46-a61f-9187fd849223 activated'
    берём последнее слово.
    """
    if pd.isna(value):
        return ""

    text = str(value).strip()
    if not text:
        return ""

    parts = text.rsplit(" ", 1)
    if len(parts) == 2:
        return parts[1].strip().lower()

    return ""


def prepare_dataframe(df: pd.DataFrame, source_file_name: str) -> pd.DataFrame:
    result = df.copy()
    result = basic_cleanup(result)
    result = protect_from_excel_formulas(result)

    validate_input_columns(result)

    result[COLUMN_NUMBER] = result[COLUMN_NUMBER].astype(str).str.strip()
    result[COLUMN_SEAL] = result[COLUMN_SEAL].astype(str).str.strip()
    result[COLUMN_OWNER] = result[COLUMN_OWNER].astype(str).str.strip()

    result[COLUMN_ISSUE_DATE] = pd.to_datetime(
        result[COLUMN_ISSUE_DATE],
        errors="coerce",
    )

    result[SOURCE_COLUMN_NAME] = source_file_name

    return result


def step_1_keep_only_activated(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    rows_before = len(result)

    statuses = result[COLUMN_NUMBER].apply(extract_status_from_number)
    result = result[statuses == "activated"].copy()

    logger.info(
        f"Шаг 1. Удалены строки со статусом не 'activated': "
        f"{rows_before - len(result)}"
    )
    logger.info(f"Шаг 1. Осталось строк: {len(result)}")

    return result


def step_2_filter_date_range(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    rows_before = len(result)

    result = result[result[COLUMN_ISSUE_DATE].notna()].copy()
    result = result[
        (result[COLUMN_ISSUE_DATE] >= DATE_FROM) &
        (result[COLUMN_ISSUE_DATE] <= DATE_TO)
    ].copy()

    result = result.sort_values(by=COLUMN_ISSUE_DATE, ascending=True).copy()

    logger.info(
        f"Шаг 2. Удалены строки вне диапазона дат "
        f"{DATE_FROM.strftime('%d.%m.%Y')} - {DATE_TO.strftime('%d.%m.%Y')}: "
        f"{rows_before - len(result)}"
    )
    logger.info(f"Шаг 2. Осталось строк: {len(result)}")

    return result


def build_owner_list(df: pd.DataFrame, owner_name: str) -> pd.DataFrame:
    """
    Возвращает 2 колонки:
    - Дата выдачи
    - Пломбы
    """
    result = df[df[COLUMN_OWNER] == owner_name].copy()
    result = result.sort_values(by=COLUMN_ISSUE_DATE, ascending=True).copy()

    result = result[[COLUMN_ISSUE_DATE, COLUMN_SEAL]].copy()
    result = result.rename(columns={COLUMN_SEAL: "Пломбы"})

    return result


def build_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Лист 4:
        | Месяц    | ИКТТ | Arrowtech | Всего |
        | Февраль  |  ... |    ...    |  ...  |
        | Март     |  ... |    ...    |  ...  |
        | Апрель   |  ... |    ...    |  ...  |
    """
    work = df.copy()
    work["month_num"] = work[COLUMN_ISSUE_DATE].dt.month

    rows = []

    for month_num in [2, 3, 4]:
        month_name = MONTH_NAMES_RU[month_num]

        iktt_count = len(
            work[
                (work["month_num"] == month_num) &
                (work[COLUMN_OWNER] == OWNER_IKTT)
            ]
        )

        arrowtech_count = len(
            work[
                (work["month_num"] == month_num) &
                (work[COLUMN_OWNER] == OWNER_ARROWTECH)
            ]
        )

        total_count = iktt_count + arrowtech_count

        rows.append({
            "Месяц": month_name,
            "ИКТТ": iktt_count,
            "Arrowtech": arrowtech_count,
            "Всего": total_count,
        })

    return pd.DataFrame(rows)


def build_period_owner_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    Возвращает список за период с 11.02.2026 по 11.03.2026 включительно
    с 3 колонками:
    - Дата выдачи
    - Пломбы
    - Владелец
    """
    result = df.copy()

    result = result[
        (result[COLUMN_ISSUE_DATE] >= PERIOD_FROM) &
        (result[COLUMN_ISSUE_DATE] <= PERIOD_TO)
    ].copy()

    result = result.sort_values(by=COLUMN_ISSUE_DATE, ascending=True).copy()

    result = result[[COLUMN_ISSUE_DATE, COLUMN_SEAL, COLUMN_OWNER]].copy()
    result = result.rename(columns={
        COLUMN_SEAL: "Пломбы",
        COLUMN_OWNER: "Владелец",
    })

    return result


def run():
    input_file = get_single_input_file(INPUT_PATTERNS)
    output_file_name = build_output_file_name(OUTPUT_FILE_BASE_NAME)
    output_file = OUT_DIR / output_file_name

    logger.info(f"Найден входной файл: {input_file.name}")

    if input_file.suffix.lower() == ".csv":
        df_raw = read_table(input_file)
    else:
        df_raw = read_excel(input_file, sheet_name=INPUT_SHEET_NAME)

    logger.info(
        f"Лист 1 прочитан: строк={len(df_raw)}, "
        f"колонок={len(df_raw.columns)}"
    )

    prepared_df = prepare_dataframe(df_raw, input_file.name)
    logger.info(f"После подготовки данных строк: {len(prepared_df)}")

    step1_df = step_1_keep_only_activated(prepared_df)
    step2_df = step_2_filter_date_range(step1_df)

    arrowtech_df = build_owner_list(step2_df, OWNER_ARROWTECH)
    iktt_df = build_owner_list(step2_df, OWNER_IKTT)
    summary_df = build_summary_table(step2_df)
    period_df = build_period_owner_list(step2_df)

    logger.info(f"Лист 2. Arrowtech: {len(arrowtech_df)} строк")
    logger.info(f"Лист 3. ИКТТ: {len(iktt_df)} строк")
    logger.info(f"Лист 4. Итоговая таблица: {len(summary_df)} строк")
    logger.info(f"Лист 5. Период 11.02.2026-11.03.2026: {len(period_df)} строк")

    sheets_to_write = {
        OUTPUT_SHEET_RAW_NAME: prepared_df,
        OUTPUT_SHEET_ARROWTECH_NAME: arrowtech_df,
        OUTPUT_SHEET_IKTT_NAME: iktt_df,
        OUTPUT_SHEET_SUMMARY_NAME: summary_df,
        OUTPUT_SHEET_PERIOD_NAME: period_df,
    }

    write_excel_sheets(
        sheets=sheets_to_write,
        file_path=output_file,
        index=False,
    )

    logger.info(f"Результат записан: {output_file}")