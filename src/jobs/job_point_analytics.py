from __future__ import annotations

from datetime import datetime
import re
import sys

import pandas as pd

from src.core.config import OUT_DIR
from src.core.logger import get_logger
from src.io.io_excel import find_input_files, read_table, write_excel_sheets
from src.processing.transform import protect_from_excel_formulas


logger = get_logger(__name__)


INPUT_PATTERNS = [
    "Выгрузка*.xlsx",
    "Выгрузка*.xls",
    "Выгрузка*.csv",
    "export*.xlsx",
    "export*.xls",
    "export*.csv",
]

OUTPUT_FILE_BASE_NAME = "report"

COLUMN_PLATES = "План установки пломб"
COLUMN_STATUS_DATE = "Status date"
COLUMN_START_WAYPOINT = "Start waypoint"
COLUMN_END_WAYPOINT = "End waypoint"

SHEET_SUMMARY = "Аналитика точек"
SHEET_DETAILS = "Детали точек"
SHEET_MONTHLY = "Оборот по месяцам"

MODE_START = "1"
MODE_END = "2"

POINT_MODES = {
    MODE_START: {
        "column": COLUMN_START_WAYPOINT,
        "direction": "start",
        "title": "стартовых точек",
        "raw_column": "Исходная стартовая точка",
        "group_column": "Группа стартовой точки",
        "output_base": "report_start_points",
    },
    MODE_END: {
        "column": COLUMN_END_WAYPOINT,
        "direction": "end",
        "title": "конечных точек",
        "raw_column": "Исходная конечная точка",
        "group_column": "Группа конечной точки",
        "output_base": "report_end_points",
    },
}

CUSTOMS_MARKERS = (
    "т/п",
    "тп ",
    "таможенный пост",
    "центр таможенного оформления",
    "цто",
)

ALMATY_GROUP = "Казахстан, Алматы и Алматинская область"

REPUBLIC_CITY_ALIASES = {
    "астана": "Казахстан, Астана",
    "г. астана": "Казахстан, Астана",
    "город астана": "Казахстан, Астана",
    "нур-султан": "Казахстан, Астана",
    "нурсултан": "Казахстан, Астана",
    "алматы": ALMATY_GROUP,
    "г. алматы": ALMATY_GROUP,
    "город алматы": ALMATY_GROUP,
    "шымкент": "Казахстан, Шымкент",
    "г. шымкент": "Казахстан, Шымкент",
    "город шымкент": "Казахстан, Шымкент",
}

CITY_TO_GROUP = {
    "актобе": "Казахстан, Актюбинская область",
    "кокшетау": "Казахстан, Акмолинская область",
    "петропавловск": "Казахстан, Северо-Казахстанская область",
    "караганда": "Казахстан, Карагандинская область",
    "костанай": "Казахстан, Костанайская область",
    "павлодар": "Казахстан, Павлодарская область",
    "усть-каменогорск": "Казахстан, Восточно-Казахстанская область",
    "оскемен": "Казахстан, Восточно-Казахстанская область",
    "семей": "Казахстан, Абайская область",
    "кызылорда": "Казахстан, Кызылординская область",
    "тараз": "Казахстан, Жамбылская область",
    "туркестан": "Казахстан, Туркестанская область",
    "талдыкорган": "Казахстан, Жетысуская область",
    "конаев": ALMATY_GROUP,
    "атырау": "Казахстан, Атырауская область",
    "актау": "Казахстан, Мангистауская область",
    "уральск": "Казахстан, Западно-Казахстанская область",
    "орал": "Казахстан, Западно-Казахстанская область",
}


def console_info(message: str) -> None:
    print(f"[job_point_analytics] {message}", flush=True)


def build_output_file_name(base_name: str) -> str:
    return f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"


def normalize_spaces(value: str) -> str:
    text = str(value).replace("\n", " ").replace("\r", " ")
    text = text.replace("«", '"').replace("»", '"').replace("'", '"')
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_for_match(value: str) -> str:
    text = normalize_spaces(value).lower()
    text = text.replace("–", "-").replace("—", "-")
    return text.strip()


def title_text(value: str) -> str:
    text = normalize_spaces(value)
    return text[:1].upper() + text[1:].lower() if text else text


def extract_plate_numbers(value) -> list[str]:
    if pd.isna(value):
        return []

    if isinstance(value, (int, float)) and float(value).is_integer():
        return [str(int(value))]

    text = normalize_spaces(str(value))
    if not text:
        return []

    if re.fullmatch(r"\d+\.0", text):
        return [text[:-2]]

    return re.findall(r"\d+", text)


def get_plate_columns(df: pd.DataFrame) -> list[str]:
    plate_columns = [
        column
        for column in df.columns
        if str(column) == COLUMN_PLATES or str(column).startswith(f"{COLUMN_PLATES}.")
    ]

    if not plate_columns:
        raise KeyError(
            f"В исходной таблице отсутствуют колонки с пломбами: {COLUMN_PLATES}"
        )

    return plate_columns


def extract_plate_numbers_from_row(row: pd.Series, plate_columns: list[str]) -> list[str]:
    plates = []

    for column in plate_columns:
        plates.extend(extract_plate_numbers(row.get(column)))

    return plates


def normalize_status_date(value):
    if pd.isna(value):
        return pd.NaT

    return pd.to_datetime(value, errors="coerce")


def format_month_key(value) -> str:
    date_value = normalize_status_date(value)
    if pd.isna(date_value):
        return "Без даты"

    return date_value.to_period("M").strftime("%Y-%m")


def is_customs_point(value: str) -> bool:
    text = normalize_for_match(value)
    return any(marker in text for marker in CUSTOMS_MARKERS)


def normalize_customs_point(value: str) -> str:
    text = normalize_spaces(value)

    quoted = re.search(r'"([^"]+)"', text)
    if quoted:
        return f'Т/П "{quoted.group(1).strip().upper()}"'

    upper_text = text.upper()
    tp_match = re.search(r"(Т/П|ТП)\s+(.+)", upper_text)
    if tp_match:
        return f"Т/П {tp_match.group(2).strip()}"

    post_match = re.search(r"(ТАМОЖЕННЫЙ ПОСТ|ЦЕНТР ТАМОЖЕННОГО ОФОРМЛЕНИЯ)\s+(.+)", upper_text)
    if post_match:
        return f"Т/П {post_match.group(2).strip()}"

    upper_text = re.sub(r"\s+", " ", upper_text).strip()
    return upper_text


def match_known_city(value: str) -> str | None:
    text = normalize_for_match(value)
    compact_text = re.sub(r"[^0-9a-zа-яё-]+", " ", text)

    for city, group in CITY_TO_GROUP.items():
        if re.search(rf"\b{re.escape(city)}\b", compact_text):
            return group

    for alias, group in REPUBLIC_CITY_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", compact_text):
            return group

    return None


def normalize_point_group_name(value: str) -> str:
    normalized = normalize_for_match(value)
    if normalized in {
        "казахстан, алматы",
        "казахстан, алматинская область",
        "алматы",
        "алматинская область",
    }:
        return ALMATY_GROUP

    return value


def match_region(value: str) -> str | None:
    text = normalize_spaces(value)
    region_match = re.search(
        r"([А-Яа-яЁё-]+(?:\s+[А-Яа-яЁё-]+)?\s+область)",
        text,
        flags=re.IGNORECASE,
    )
    if region_match:
        return normalize_point_group_name(f"Казахстан, {title_text(region_match.group(1))}")

    return None


def normalize_kazakhstan_address(parts: list[str]) -> str:
    if len(parts) == 1:
        return "Казахстан"

    second = normalize_for_match(parts[1])
    if second in REPUBLIC_CITY_ALIASES:
        return REPUBLIC_CITY_ALIASES[second]

    if "область" in second:
        return normalize_point_group_name(f"Казахстан, {title_text(parts[1])}")

    if len(parts) >= 3:
        third = normalize_for_match(parts[2])
        if third in REPUBLIC_CITY_ALIASES:
            return REPUBLIC_CITY_ALIASES[third]

        if "область" in third:
            return normalize_point_group_name(f"Казахстан, {title_text(parts[2])}")

        return normalize_point_group_name(f"Казахстан, {title_text(parts[1])}")

    return normalize_point_group_name(f"Казахстан, {title_text(parts[1])}")


def normalize_waypoint(value) -> str:
    if pd.isna(value):
        return "Не указано"

    text = normalize_spaces(str(value))
    if not text:
        return "Не указано"

    if is_customs_point(text):
        return normalize_customs_point(text)

    parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        return "Не указано"

    country = normalize_for_match(parts[0])
    if country in {"казахстан", "республика казахстан", "rk", "kz"}:
        return normalize_kazakhstan_address(parts)

    region_group = match_region(text)
    if region_group:
        return region_group

    city_group = match_known_city(text)
    if city_group:
        return city_group

    if len(parts) >= 2:
        return f"{title_text(parts[0])}, {title_text(parts[1])}"

    return title_text(parts[0])


def validate_input_columns(df: pd.DataFrame, point_column: str) -> list[str]:
    plate_columns = get_plate_columns(df)
    required_columns = {COLUMN_STATUS_DATE, point_column}
    missing = [column for column in required_columns if column not in df.columns]

    if missing:
        raise KeyError(f"В исходной таблице отсутствуют обязательные колонки: {missing}")

    return plate_columns


def get_latest_input_file():
    files = find_input_files(INPUT_PATTERNS)
    latest_file = max(files, key=lambda path: path.stat().st_mtime)

    if len(files) > 1:
        logger.info(
            "Найдено несколько входных файлов, выбран самый свежий: "
            f"{latest_file.name}"
        )
        console_info(
            "Найдено несколько входных файлов, беру самый свежий: "
            f"{latest_file.name}"
        )

    return latest_file


def select_point_mode() -> dict:
    cli_mode = sys.argv[2].strip() if len(sys.argv) >= 3 else ""

    if cli_mode in {"start", "Start", "START"}:
        cli_mode = MODE_START
    elif cli_mode in {"end", "End", "END"}:
        cli_mode = MODE_END

    if cli_mode in POINT_MODES:
        return POINT_MODES[cli_mode]

    console_info("Выберите режим обработки:")
    console_info("1 - Start waypoint: откуда больше всего уезжает пломб")
    console_info("2 - End waypoint: куда больше всего приезжает пломб")

    selected = input("Введите 1 или 2: ").strip()
    if selected not in POINT_MODES:
        raise ValueError("Нужно выбрать 1 для Start waypoint или 2 для End waypoint")

    return POINT_MODES[selected]


def build_point_rows(
    df: pd.DataFrame,
    mode: dict,
    plate_columns: list[str],
) -> pd.DataFrame:
    rows = []
    point_column = mode["column"]
    group_column = mode["group_column"]
    raw_column = mode["raw_column"]

    for row_index, row in df.iterrows():
        plates = extract_plate_numbers_from_row(row, plate_columns)
        if not plates:
            continue

        raw_point = normalize_spaces(str(row.get(point_column, "")))
        point_group = normalize_waypoint(row.get(point_column))
        status_date = normalize_status_date(row.get(COLUMN_STATUS_DATE))
        status_date_text = "" if pd.isna(status_date) else status_date.strftime("%Y-%m-%d")
        status_month = format_month_key(row.get(COLUMN_STATUS_DATE))

        rows.append(
            {
                group_column: point_group,
                raw_column: raw_point if raw_point else "Не указано",
                "Status date": status_date_text,
                "Месяц Status date": status_month,
                "Количество пломб": len(plates),
                "Номера пломб": ", ".join(plates),
                "Номер строки": row_index + 2,
            }
        )

    return pd.DataFrame(rows)


def build_summary(point_rows: pd.DataFrame, mode: dict) -> pd.DataFrame:
    group_column = mode["group_column"]
    raw_column = mode["raw_column"]

    if point_rows.empty:
        return pd.DataFrame(
            columns=[
                group_column,
                "Количество пломб",
                "Количество разных исходных точек",
            ]
        )

    summary = (
        point_rows.groupby(group_column, dropna=False)
        .agg(
            **{
                "Количество пломб": ("Количество пломб", "sum"),
                "Количество разных исходных точек": (raw_column, "nunique"),
            }
        )
        .reset_index()
        .sort_values(
            by=["Количество пломб", group_column],
            ascending=[False, True],
        )
        .reset_index(drop=True)
    )

    total_row = pd.DataFrame(
        [
            {
                group_column: "ИТОГО",
                "Количество пломб": summary["Количество пломб"].sum(),
                "Количество разных исходных точек": point_rows[raw_column].nunique(),
            }
        ]
    )

    return pd.concat([summary, total_row], ignore_index=True)


def build_details(point_rows: pd.DataFrame, mode: dict) -> pd.DataFrame:
    if point_rows.empty:
        return point_rows

    group_column = mode["group_column"]
    raw_column = mode["raw_column"]

    return point_rows.sort_values(
        by=["Количество пломб", group_column, raw_column],
        ascending=[False, True, True],
    ).reset_index(drop=True)


def build_monthly_turnover(point_rows: pd.DataFrame, mode: dict) -> pd.DataFrame:
    group_column = mode["group_column"]

    if point_rows.empty:
        return pd.DataFrame(columns=[group_column, "ИТОГО"])

    pivot = point_rows.pivot_table(
        index=group_column,
        columns="Месяц Status date",
        values="Количество пломб",
        aggfunc="sum",
        fill_value=0,
    )

    month_columns = sorted(
        [column for column in pivot.columns if column != "Без даты"]
    )
    if "Без даты" in pivot.columns:
        month_columns.append("Без даты")

    pivot = pivot[month_columns]
    pivot["ИТОГО"] = pivot.sum(axis=1)
    pivot = pivot.sort_values(by="ИТОГО", ascending=False)

    total_row = pd.DataFrame(
        [pivot.sum(axis=0)],
        index=["ИТОГО"],
    )
    pivot = pd.concat([pivot, total_row])

    return pivot.reset_index().rename(columns={"index": group_column})


def run():
    try:
        logger.info("Начало выполнения job_point_analytics")
        mode = select_point_mode()
        console_info(f"Старт: считаем пломбы и группируем {mode['title']}.")

        input_file = get_latest_input_file()
        console_info(f"Входной файл: {input_file.name}")

        df = read_table(input_file, sheet_name=0)
        plate_columns = validate_input_columns(df, mode["column"])
        console_info(f"Выгрузка прочитана: {len(df)} строк")
        console_info(f"Колонки с пломбами: {len(plate_columns)}")

        point_rows = build_point_rows(df, mode, plate_columns)
        summary_df = build_summary(point_rows, mode)
        details_df = build_details(point_rows, mode)
        monthly_df = build_monthly_turnover(point_rows, mode)

        total_plates = int(point_rows["Количество пломб"].sum()) if not point_rows.empty else 0
        console_info(
            f"Найдено пломб: {total_plates}; групп {mode['title']}: {len(summary_df) - 1 if not summary_df.empty else 0}"
        )

        output_file = OUT_DIR / build_output_file_name(mode["output_base"])
        write_excel_sheets(
            {
                SHEET_SUMMARY: protect_from_excel_formulas(summary_df),
                SHEET_DETAILS: protect_from_excel_formulas(details_df),
                SHEET_MONTHLY: protect_from_excel_formulas(monthly_df),
            },
            output_file,
            index=False,
        )

        logger.info(f"Результат записан в {output_file}")
        console_info(f"Готово. Результат сохранен: {output_file}")

    except Exception as error:
        logger.exception(f"Ошибка при выполнении job_point_analytics: {error}")
        console_info(f"Ошибка: {error}")
        raise


if __name__ == "__main__":
    run()
