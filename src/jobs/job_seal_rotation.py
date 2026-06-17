from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re

import pandas as pd

from src.core.config import IN_DIR, OUT_DIR
from src.core.logger import get_logger
from src.io.io_excel import find_input_files, read_table, write_excel_sheets
from src.processing.transform import protect_from_excel_formulas


logger = get_logger(__name__)


EXPORT_PATTERNS = ["Выгрузка*.xlsx", "Выгрузка*.xls", "Выгрузка*.csv"]
FILTER_FILE_NAME = "Список.xlsx"
OUTPUT_FILE_BASE_NAME = "Коэффициент_возврата_пломб"

COLUMN_START_DATE = "Дата"
COLUMN_END_DATE = "Status date"
COLUMN_PLATES = "План установки пломб"

CONTRACT_DAYS = 20
MONTH_DAYS = 30
RETURN_OPTIONS = {
    "1": {"name": "РФ", "days": 10},
    "2": {"name": "Беларусь", "days": 10},
    "3": {"name": "Кыргызстан", "days": 3},
}

SHEET_SUMMARY = "Сводка"
SHEET_UNIQUE = "Уникальные пломбы"
SHEET_RETURNS = "Возвраты пломб"


def console_info(message: str) -> None:
    print(f"[job_seal_rotation] {message}", flush=True)


def build_output_file_name(base_name: str) -> str:
    return f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"


def normalize_plate(value) -> str:
    if pd.isna(value):
        return ""

    if isinstance(value, (int, float)) and float(value).is_integer():
        return str(int(value))

    text = str(value).strip()
    if re.fullmatch(r"\d+\.0", text):
        return text[:-2]

    return re.sub(r"\D+", "", text)


def extract_plates_from_cell(value) -> list[str]:
    if pd.isna(value):
        return []

    if isinstance(value, (int, float)) and float(value).is_integer():
        return [str(int(value))]

    text = str(value).strip()
    if not text:
        return []

    if re.fullmatch(r"\d+\.0", text):
        return [text[:-2]]

    return [
        normalize_plate(token)
        for token in re.findall(r"\d+(?:\.0)?", text)
        if normalize_plate(token)
    ]


def get_latest_export_file() -> Path:
    files = find_input_files(EXPORT_PATTERNS)
    files = [file for file in files if not file.name.startswith("~$")]
    if not files:
        raise FileNotFoundError("Не найден файл выгрузки по маске Выгрузка*.xlsx/xls/csv")

    latest_file = max(files, key=lambda path: path.stat().st_mtime)
    if len(files) > 1:
        logger.info(f"Найдено несколько выгрузок, выбран самый свежий файл: {latest_file.name}")
        console_info(f"Найдено несколько выгрузок, беру самый свежий: {latest_file.name}")

    return latest_file


def get_plate_columns(df: pd.DataFrame) -> list[str]:
    plate_columns = [
        column
        for column in df.columns
        if str(column) == COLUMN_PLATES or str(column).startswith(f"{COLUMN_PLATES}.")
    ]
    if not plate_columns:
        raise KeyError(f"В выгрузке не найдены колонки с пломбами: {COLUMN_PLATES}")

    return plate_columns


def load_filter_list() -> set[str] | None:
    filter_file = IN_DIR / FILTER_FILE_NAME
    if not filter_file.exists():
        answer = input(
            f"Файл {FILTER_FILE_NAME} не найден. "
            "Продолжить без фильтрации пломб? (Y/N): "
        ).strip().lower()
        if answer not in {"y", "yes", "д", "да"}:
            raise RuntimeError("Выполнение остановлено пользователем")
        return None

    logger.info(f"Чтение фильтра пломб из файла: {filter_file.name}")
    filter_df = read_table(filter_file, sheet_name=0, header=None)
    if filter_df.empty:
        logger.warning(f"Файл {FILTER_FILE_NAME} пустой, фильтр не применяется")
        return None

    first_column = filter_df.iloc[:, 0].dropna()
    values = list(first_column)
    if values and re.search(r"[A-Za-zА-Яа-яЁё]", str(values[0])):
        values = values[1:]

    result = {
        normalize_plate(value)
        for value in values
        if normalize_plate(value)
    }

    logger.info(f"Загружено пломб из фильтра: {len(result)}")
    console_info(f"Фильтр Список.xlsx: {len(result)} пломб")
    return result


def load_export() -> tuple[pd.DataFrame, Path, list[str]]:
    export_file = get_latest_export_file()
    logger.info(f"Чтение выгрузки: {export_file}")
    df = read_table(export_file, sheet_name=0)

    required_columns = {COLUMN_START_DATE, COLUMN_END_DATE}
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise KeyError(f"В выгрузке отсутствуют обязательные колонки: {missing}")

    plate_columns = get_plate_columns(df)
    logger.info(
        f"Выгрузка прочитана: строк={len(df)}, колонок={len(df.columns)}, "
        f"колонок пломб={len(plate_columns)}"
    )
    return df, export_file, plate_columns


def extract_seals(
    df: pd.DataFrame,
    plate_columns: list[str],
    filter_plates: set[str] | None = None,
) -> pd.DataFrame:
    rows = []
    use_filter = filter_plates is not None

    for row_number, row in df.iterrows():
        start_date = pd.to_datetime(row.get(COLUMN_START_DATE), errors="coerce")
        end_date = pd.to_datetime(row.get(COLUMN_END_DATE), errors="coerce")

        for column in plate_columns:
            for plate in extract_plates_from_cell(row.get(column)):
                if use_filter and plate not in filter_plates:
                    continue

                rows.append(
                    {
                        "Пломба": plate,
                        "Дата начала": start_date,
                        "Дата окончания": end_date,
                        "Номер строки выгрузки": row_number + 2,
                    }
                )

    result = pd.DataFrame(rows)
    if result.empty:
        return pd.DataFrame(
            columns=[
                "Пломба",
                "Дата начала",
                "Дата окончания",
                "Номер строки выгрузки",
                "Дней в перевозке",
            ]
        )

    result["Дней в перевозке"] = (
        result["Дата окончания"] - result["Дата начала"]
    ).dt.total_seconds() / 86400
    result = result.sort_values(
        by=["Пломба", "Дата начала", "Дата окончания"],
        ascending=[True, True, True],
    ).reset_index(drop=True)

    logger.info(f"Извлечено записей пломб после фильтрации: {len(result)}")
    return result


def calculate_transport_stats(seal_rows: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    valid = seal_rows[
        seal_rows["Дата начала"].notna()
        & seal_rows["Дата окончания"].notna()
        & seal_rows["Дней в перевозке"].notna()
        & (seal_rows["Дней в перевозке"] >= 0)
    ].copy()

    if valid.empty:
        unique_df = pd.DataFrame(
            columns=["Пломба", "Дата начала", "Дата окончания", "Дней в перевозке"]
        )
    else:
        unique_df = (
            valid.sort_values(by=["Пломба", "Дата начала", "Дата окончания"])
            .drop_duplicates(subset=["Пломба"], keep="first")
            [["Пломба", "Дата начала", "Дата окончания", "Дней в перевозке"]]
            .reset_index(drop=True)
        )

    stats = build_numeric_stats(unique_df["Дней в перевозке"])
    return unique_df, stats


def calculate_return_stats(seal_rows: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    valid = seal_rows[
        seal_rows["Дата начала"].notna()
        & seal_rows["Дата окончания"].notna()
    ].copy()

    return_rows = []
    for plate, group in valid.groupby("Пломба"):
        group = group.sort_values(by=["Дата начала", "Дата окончания"])
        records = list(group[["Дата начала", "Дата окончания"]].itertuples(index=False))

        for previous, current in zip(records, records[1:]):
            previous_end = previous[1]
            next_start = current[0]
            return_days = (next_start - previous_end).total_seconds() / 86400

            if return_days < 0:
                continue

            return_rows.append(
                {
                    "Пломба": plate,
                    "Конец предыдущей перевозки": previous_end,
                    "Начало следующей перевозки": next_start,
                    "Дней возврата": return_days,
                }
            )

    returns_df = pd.DataFrame(
        return_rows,
        columns=[
            "Пломба",
            "Конец предыдущей перевозки",
            "Начало следующей перевозки",
            "Дней возврата",
        ],
    )

    stats = build_numeric_stats(returns_df["Дней возврата"])
    return returns_df, stats


def build_numeric_stats(series: pd.Series) -> dict:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {"avg": None, "min": None, "max": None, "count": 0}

    return {
        "avg": float(values.mean()),
        "min": float(values.min()),
        "max": float(values.max()),
        "count": int(values.count()),
    }


def format_number(value, ndigits: int = 2):
    if value is None or pd.isna(value):
        return ""
    return round(float(value), ndigits)


def select_return_option() -> dict:
    console_info("Выберите направление возврата для расчета коэффициента:")
    console_info("1 - РФ: 10 дней возврата")
    console_info("2 - Беларусь: 10 дней возврата")
    console_info("3 - Кыргызстан: 3 дня возврата")

    selected = input("Введите 1, 2 или 3: ").strip()
    if selected not in RETURN_OPTIONS:
        raise ValueError("Нужно выбрать 1, 2 или 3 для направления возврата")

    option = RETURN_OPTIONS[selected]
    logger.info(
        f"Выбрано направление возврата: {option['name']}, "
        f"дней возврата={option['days']}"
    )
    return option


def calculate_rotation_coef(transport_stats: dict, return_option: dict) -> dict:
    avg_transport_days = transport_stats["avg"]
    return_days = return_option["days"]

    if avg_transport_days is None:
        cycle_days = None
        rotation_coef = None
    else:
        cycle_days = avg_transport_days + CONTRACT_DAYS + return_days
        rotation_coef = cycle_days / MONTH_DAYS

    return {
        "avg_transport_days": avg_transport_days,
        "return_country": return_option["name"],
        "return_days": return_days,
        "contract_days": CONTRACT_DAYS,
        "cycle_days": cycle_days,
        "rotation_coef": rotation_coef,
    }


def build_summary_sheet(
    export_file: Path,
    filter_plates: set[str] | None,
    seal_rows: pd.DataFrame,
    unique_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    transport_stats: dict,
    return_stats: dict,
    rotation: dict,
) -> pd.DataFrame:
    filter_text = (
        f"Да, {len(filter_plates)} пломб в Список.xlsx"
        if filter_plates is not None
        else "Нет, обработаны все пломбы выгрузки"
    )

    rows = [
        ("Файл выгрузки", export_file.name),
        ("Фильтр по Список.xlsx", filter_text),
        ("Количество записей пломб после фильтрации", len(seal_rows)),
        ("Количество уникальных пломб", len(unique_df)),
        ("Количество найденных интервалов возврата", len(returns_df)),
        ("Среднее время перевозки, дней", format_number(transport_stats["avg"])),
        ("Минимальное время перевозки, дней", format_number(transport_stats["min"])),
        ("Максимальное время перевозки, дней", format_number(transport_stats["max"])),
        ("Направление возврата для коэффициента", rotation["return_country"]),
        ("Дней возврата для коэффициента", rotation["return_days"]),
        ("Договорной срок, дней", CONTRACT_DAYS),
        ("Полный цикл пломбы, дней", format_number(rotation["cycle_days"])),
        ("Коэффициент ротации", format_number(rotation["rotation_coef"])),
        (
            "Формула",
            "Коэффициент ротации = (Среднее время перевозки, дней + "
            "Договорной срок, дней + Дней возврата для коэффициента) / 30",
        ),
    ]

    return pd.DataFrame(rows, columns=["Показатель", "Значение"])


def build_report(
    summary_df: pd.DataFrame,
    unique_df: pd.DataFrame,
    returns_df: pd.DataFrame,
) -> Path:
    output_file = OUT_DIR / build_output_file_name(OUTPUT_FILE_BASE_NAME)
    write_excel_sheets(
        {
            SHEET_SUMMARY: protect_from_excel_formulas(summary_df),
            SHEET_UNIQUE: protect_from_excel_formulas(unique_df),
            SHEET_RETURNS: protect_from_excel_formulas(returns_df),
        },
        output_file,
        index=False,
    )
    return output_file


def run():
    try:
        logger.info("Начало выполнения job_seal_rotation")
        console_info("Старт расчета коэффициента возврата пломб")

        export_df, export_file, plate_columns = load_export()
        filter_plates = load_filter_list()
        return_option = select_return_option()

        seal_rows = extract_seals(export_df, plate_columns, filter_plates)
        unique_df, transport_stats = calculate_transport_stats(seal_rows)
        returns_df, return_stats = calculate_return_stats(seal_rows)
        rotation = calculate_rotation_coef(transport_stats, return_option)

        summary_df = build_summary_sheet(
            export_file=export_file,
            filter_plates=filter_plates,
            seal_rows=seal_rows,
            unique_df=unique_df,
            returns_df=returns_df,
            transport_stats=transport_stats,
            return_stats=return_stats,
            rotation=rotation,
        )
        output_file = build_report(summary_df, unique_df, returns_df)

        console_info(f"Уникальных пломб: {len(unique_df)}")
        console_info(
            "Среднее время перевозки: "
            f"{format_number(transport_stats['avg'])} дней"
        )
        console_info(
            "Возврат для коэффициента: "
            f"{rotation['return_country']}, {rotation['return_days']} дней"
        )
        console_info(
            "Коэффициент ротации: "
            f"{format_number(rotation['rotation_coef'])}"
        )
        console_info(f"Готово. Результат сохранен: {output_file}")

        logger.info(f"Результат записан: {output_file}")

    except Exception as error:
        logger.exception(f"Ошибка при выполнении job_seal_rotation: {error}")
        console_info(f"Ошибка: {error}")
        raise


if __name__ == "__main__":
    run()
