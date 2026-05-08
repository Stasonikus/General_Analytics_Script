from datetime import datetime
import json
import re
from pathlib import Path

import pandas as pd

from src.core.config import LOCATIONS_PATHS, OUT_DIR
from src.io.io_excel import get_single_input_file, read_excel, write_excel_sheets
from src.processing.transform import protect_from_excel_formulas
from src.core.logger import get_logger

logger = get_logger(__name__)


def console_info(message: str) -> None:
    """Печатает короткий статус обработки в консоль."""
    print(f"[jobPROVERCA_NP] {message}", flush=True)


# ----- Конфигурация -----
PATTERN_LIST = ["Список о НП*.xlsx", "Список о НП*.xls", "Список о НП*.csv"]
PATTERN_EXTRACT = ["Выгрузка*.xlsx", "Выгрузка*.xls", "Выгрузка*.csv"]

OUTPUT_FILE_BASE_NAME = "result_compare_plates"
COLUMN_PLATES = "План установки пломб"
COLUMN_END_WAYPOINT = "End waypoint"
COLUMN_DESTINATION_COUNTRY = "Пункт назначения пломбы"

SHEET_STATS = "Статистика"
SHEET_NOT_FOUND = "Не найдены"
SHEET_FOUND = "Найдены"
SHEET_FOUND_DETAILS = "Детали совпадений"

COUNTRY_LABELS = {
    "KZ": "КЗ",
    "RB": "РБ",
    "RF": "РФ",
}

COUNTRY_ORDER = ["RB", "RF", "KZ"]

MIN_COMPACT_LOCATION_LENGTH = 5
NON_TARGET_COUNTRY_MARKERS = (
    # Кыргызстан
    "бишкек",
    "кыргыз",
    "киргиз",
    "киргизия",
    "кыргызстан",
    "kgz",
    "kg",
    "kyrgyz",
    "bishkek",
    "osh",
    "ош",
    "джалал",
    "jalal",
    "нарын",
    "talas",
    "талас",
    "иссык",
    "issykkul",
    "кок-джар",
    "кок джар",
    "кокджар",
    "баялинова",
)

DETAIL_COLUMNS = [
    "№",
    "Дата",
    "Владелец записи",
    "Уникальный номер перевозки",
    "Статус",
    "Status date",
    "Тип",
    "Компания",
    "БИН",
    "Файлы",
    "Автомобили",
    "Коды ТН ВЭД",
    "Путевые точки",
    "План установки пломб",
    "Работает пломб",
    "Водители",
    "Решения",
    "Меры контроля",
    "Операторы",
    "Описание",
    "Start waypoint",
    "End waypoint",
    "company_id",
]

# ----- Вспомогательные функции -----
def build_output_file_name(base_name: str) -> str:
    current_date = datetime.now().strftime("%Y%m%d")
    return f"{base_name}_{current_date}.xlsx"

def extract_numeric_plates_from_cell(cell_value) -> set:
    """Извлекает из ячейки все цифровые последовательности (номера пломб)."""
    if pd.isna(cell_value):
        return set()
    cell_str = str(cell_value).strip()
    if not cell_str:
        return set()
    parts = cell_str.split(',')
    result = set()
    for part in parts:
        part = part.strip()
        numbers = re.findall(r'\d+', part)
        for num in numbers:
            result.add(num)
    return result

def normalize_location_text(value) -> str:
    """Готовит текст локации к устойчивому сравнению."""
    if pd.isna(value):
        return ""

    text = str(value).strip().lower()
    text = text.replace("–", "-").replace("—", "-")
    text = text.replace("«", '"').replace("»", '"')
    text = re.sub(r"\s+", " ", text)
    return text

def compact_location_text(value) -> str:
    """Убирает служебные символы для сравнения коротких вариантов написания."""
    text = normalize_location_text(value)
    text = text.replace("т/п", "")
    text = text.replace("тп", "")
    text = re.sub(r"[^0-9a-zа-яё]+", "", text)
    return text

def load_locations_by_country() -> dict[str, list[tuple[str, ...]]]:
    """Загружает JSON-справочники локаций из config.py."""
    locations_by_country: dict[str, list[tuple[str, ...]]] = {}

    for country_code, file_path in LOCATIONS_PATHS.items():
        if not file_path.exists():
            logger.warning(f"Файл справочника локаций не найден: {file_path}")
            locations_by_country[country_code] = []
            continue

        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        seen_locations = set()
        country_locations = []

        for location in data.get("locations", []):
            normalized = normalize_location_text(location)
            compact = compact_location_text(location)
            location_keys = tuple(
                key
                for key in (normalized, compact)
                if key and len(key) >= MIN_COMPACT_LOCATION_LENGTH
            )

            if not location_keys or location_keys in seen_locations:
                continue

            seen_locations.add(location_keys)
            country_locations.append(location_keys)

        locations_by_country[country_code] = country_locations

        logger.info(
            f"Загружен справочник {country_code}: {len(locations_by_country[country_code])} локаций"
        )
        console_info(
            f"Справочник {COUNTRY_LABELS.get(country_code, country_code)}: "
            f"{len(locations_by_country[country_code])} уникальных локаций"
        )

    return locations_by_country

def location_key_matches_waypoint(location_key: str, waypoint: str, compact_waypoint: str) -> bool:
    """
    Более безопасное сравнение локаций.

    Защита от ложных совпадений:
    - РФ не должна матчиться по случайным кускам строки
    - короткие фрагменты не учитываются
    - сравнение идет по словам и нормализованным строкам
    """

    if not location_key:
        return False

    normalized_key = normalize_location_text(location_key)
    compact_key = compact_location_text(location_key)

    # Полное текстовое совпадение
    if normalized_key and normalized_key in waypoint:
        return True

    # Для compact-сравнения используем только достаточно длинные значения
    if len(compact_key) < 8:
        return False

    if compact_key and compact_key in compact_waypoint:
        return True

    return False

def detect_destination_country(end_waypoint, locations_by_country: dict[str, list[tuple[str, ...]]]) -> str:
    """
    Определяет страну End waypoint по справочникам KZ/RB/RF.

    Важно:
    - Кыргызстан и другие нецелевые страны должны возвращать "другая страна"
    - избегаем ложного определения РФ по частичному совпадению строки
    """

    waypoint = normalize_location_text(end_waypoint)
    compact_waypoint = compact_location_text(end_waypoint)

    if not waypoint:
        return "другая страна"

    # Сначала проверяем маркеры Кыргызстана и других нецелевых стран
    for marker in NON_TARGET_COUNTRY_MARKERS:
        normalized_marker = normalize_location_text(marker)
        compact_marker = compact_location_text(marker)

        if (
            normalized_marker and normalized_marker in waypoint
        ) or (
            compact_marker and compact_marker in compact_waypoint
        ):
            return "другая страна"

    detected = []

    for country_code in COUNTRY_ORDER:
        for location_keys in locations_by_country.get(country_code, []):
            matched = False

            for location_key in location_keys:
                if location_key_matches_waypoint(
                    location_key,
                    waypoint,
                    compact_waypoint,
                ):
                    matched = True
                    break

            if matched:
                detected.append(COUNTRY_LABELS.get(country_code, country_code))
                break

    # Удаляем дубликаты сохраняя порядок
    detected = list(dict.fromkeys(detected))

    return ", ".join(detected) if detected else "другая страна"

def read_plates_list(file_path: Path) -> set:
    """
    Читает первый столбец первого листа файла со списком пломб.
    Если первая строка не является числом (содержит буквы), она автоматически пропускается как заголовок.
    Возвращает множество строк (номера пломб), очищенных от пробелов и содержащих только цифры.
    """
    logger.info(f"Чтение списка пломб из файла: {file_path.name}")
    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path, header=None)
    else:
        df = pd.read_excel(file_path, sheet_name=0, header=None)
    
    # Берём первый столбец, удаляем NaN, преобразуем в строку
    plates_series = df.iloc[:, 0].dropna().astype(str).str.strip()
    
    # Если первая строка содержит нецифровые символы (буквы, знаки), считаем её заголовком и пропускаем
    if len(plates_series) > 0:
        first_val = plates_series.iloc[0]
        if not first_val.isdigit():
            logger.info(f"Обнаружен заголовок '{first_val}', пропускаем первую строку")
            plates_series = plates_series.iloc[1:]
    
    # Из каждой строки извлекаем только цифры (на случай, если попались символы)
    plates_set = set()
    for val in plates_series:
        nums = re.findall(r'\d+', val)
        if nums:
            plates_set.add(nums[0])
    
    logger.info(f"Прочитано уникальных пломб из списка: {len(plates_set)}")
    return plates_set

def read_extraction_table(file_path: Path) -> pd.DataFrame:
    """Читает таблицу выгрузки и проверяет наличие колонки с пломбами."""
    logger.info(f"Чтение выгрузки из файла: {file_path.name}")
    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path)
    else:
        df = read_excel(file_path, sheet_name=0)

    if COLUMN_PLATES not in df.columns:
        raise KeyError(f"В выгрузке отсутствует обязательная колонка '{COLUMN_PLATES}'")
    if COLUMN_END_WAYPOINT not in df.columns:
        raise KeyError(f"В выгрузке отсутствует обязательная колонка '{COLUMN_END_WAYPOINT}'")

    return df

def extract_plates_from_extraction_df(df: pd.DataFrame) -> tuple[set, dict]:
    """Извлекает пломбы из выгрузки.
       Возвращает (множество уникальных номеров, словарь {номер: количество вхождений}).
    """
    plate_counts = {}
    for cell in df[COLUMN_PLATES]:
        plates_in_cell = extract_numeric_plates_from_cell(cell)
        for plate in plates_in_cell:
            plate_counts[plate] = plate_counts.get(plate, 0) + 1
    unique_plates = set(plate_counts.keys())
    logger.info(f"Из выгрузки извлечено уникальных номеров пломб: {len(unique_plates)}")
    return unique_plates, plate_counts

def read_extraction_plates(file_path: Path) -> tuple[set, dict]:
    """Читает колонку 'План установки пломб' из файла выгрузки.
       Возвращает (множество уникальных номеров, словарь {номер: количество вхождений}).
    """
    df = read_extraction_table(file_path)
    return extract_plates_from_extraction_df(df)

def build_plate_destination_map(
    extraction_df: pd.DataFrame,
    found_plates: set,
    locations_by_country: dict[str, list[str]],
) -> dict[str, str]:
    """Строит словарь {номер пломбы: страна назначения по End waypoint}."""
    destinations_by_plate: dict[str, set[str]] = {}

    for _, row in extraction_df.iterrows():
        row_plates = extract_numeric_plates_from_cell(row.get(COLUMN_PLATES))
        matched_plates = row_plates.intersection(found_plates)

        if not matched_plates:
            continue

        destination_country = detect_destination_country(
            row.get(COLUMN_END_WAYPOINT),
            locations_by_country,
        )

        for plate in matched_plates:
            destinations_by_plate.setdefault(plate, set()).add(destination_country)

    ordered_labels = [COUNTRY_LABELS[code] for code in COUNTRY_ORDER]
    ordered_labels.append("другая страна")

    result = {}
    for plate, destinations in destinations_by_plate.items():
        ordered_destinations = [
            label
            for label in ordered_labels
            if label in destinations
        ]
        result[plate] = ", ".join(ordered_destinations) if ordered_destinations else "другая страна"

    return result

def build_found_details_df(extraction_df: pd.DataFrame, found_plates: set) -> pd.DataFrame:
    """
    Возвращает полные строки выгрузки, где в колонке с планом установки пломб
    есть хотя бы одна пломба из списка найденных совпадений.
    """
    rows = []

    for _, row in extraction_df.iterrows():
        row_plates = extract_numeric_plates_from_cell(row.get(COLUMN_PLATES))
        matched_plates = sorted(row_plates.intersection(found_plates))

        if not matched_plates:
            continue

        row_data = row.copy()
        row_data["Совпавшие пломбы"] = ", ".join(matched_plates)
        rows.append(row_data)

    if not rows:
        return pd.DataFrame(columns=[*DETAIL_COLUMNS, "Совпавшие пломбы"])

    details_df = pd.DataFrame(rows)

    existing_detail_columns = [col for col in DETAIL_COLUMNS if col in details_df.columns]
    missing_detail_columns = [col for col in DETAIL_COLUMNS if col not in details_df.columns]
    if missing_detail_columns:
        logger.warning(f"В выгрузке не найдены ожидаемые колонки для 4-го листа: {missing_detail_columns}")

    extra_columns = [
        col
        for col in details_df.columns
        if col not in existing_detail_columns and col != "Совпавшие пломбы"
    ]
    ordered_columns = [*existing_detail_columns, *extra_columns, "Совпавшие пломбы"]

    return details_df[ordered_columns]

# ----- Основная функция run (точка входа для job) -----
def run():
    try:
        logger.info("Начало выполнения jobPROVERCA_NP")
        console_info("Старт сравнения пломб. Сейчас найдем входные файлы и подготовим справочники.")
        
        # 1. Поиск входных файлов
        list_file = get_single_input_file(PATTERN_LIST)
        extract_file = get_single_input_file(PATTERN_EXTRACT)
        console_info(f"Список пломб: {list_file.name}")
        console_info(f"Выгрузка для проверки: {extract_file.name}")
        
        # 2. Чтение данных
        plates_list = read_plates_list(list_file)
        console_info(f"В списке пломб найдено уникальных номеров: {len(plates_list)}")

        extraction_df = read_extraction_table(extract_file)
        console_info(f"Выгрузка прочитана: {len(extraction_df)} строк")

        system_plates, plate_counts = extract_plates_from_extraction_df(extraction_df)
        console_info(f"Из выгрузки извлечено уникальных пломб: {len(system_plates)}")

        locations_by_country = load_locations_by_country()
        
        # 3. Сравнение
        found = plates_list.intersection(system_plates)
        not_found = plates_list - system_plates
        
        logger.info(f"Найдено совпадений: {len(found)}, не найдено: {len(not_found)}")
        match_percent = len(found) / len(plates_list) * 100 if plates_list else 0
        console_info(
            f"Совпадения: {len(found)} найдено, {len(not_found)} не найдено "
            f"({match_percent:.2f}% от списка)"
        )
        
        # 4. Формирование выходного Excel-файла
        output_file_name = build_output_file_name(OUTPUT_FILE_BASE_NAME)
        output_file = OUT_DIR / output_file_name
        console_info("Определяем страну назначения по колонке End waypoint.")
        
        stats_data = {
            "Показатель": ["Всего пломб в списке", "Найдено в выгрузке", "Не найдено", "Процент совпадения"],
            "Значение": [
                len(plates_list),
                len(found),
                len(not_found),
                f"{len(found)/len(plates_list)*100:.2f}%" if plates_list else "0%"
            ]
        }
        df_stats = pd.DataFrame(stats_data)
        df_not_found = pd.DataFrame(sorted(not_found), columns=["Номер пломбы"])
        plate_destination_map = build_plate_destination_map(
            extraction_df,
            found,
            locations_by_country,
        )
        destination_summary = {}
        for destination in plate_destination_map.values():
            destination_summary[destination] = destination_summary.get(destination, 0) + 1
        if destination_summary:
            summary_text = ", ".join(
                f"{destination}: {count}"
                for destination, count in sorted(destination_summary.items())
            )
            console_info(f"Распределение найденных пломб по назначению: {summary_text}")

        found_with_counts = [
            (
                plate,
                plate_counts.get(plate, 0),
                plate_destination_map.get(plate, "другая страна"),
            )
            for plate in sorted(found)
        ]
        df_found = pd.DataFrame(
            found_with_counts,
            columns=[
                "Номер пломбы",
                "Сколько раз встретилась в выгрузке",
                COLUMN_DESTINATION_COUNTRY,
            ],
        )
        df_found_details = build_found_details_df(extraction_df, found)
        
        # Защита от формул Excel (на всякий случай)
        df_stats = protect_from_excel_formulas(df_stats)
        df_not_found = protect_from_excel_formulas(df_not_found)
        df_found = protect_from_excel_formulas(df_found)
        df_found_details = protect_from_excel_formulas(df_found_details)
        
        sheets = {
            SHEET_STATS: df_stats,
            SHEET_NOT_FOUND: df_not_found,
            SHEET_FOUND: df_found,
            SHEET_FOUND_DETAILS: df_found_details
        }
        write_excel_sheets(sheets, output_file, index=False)
        logger.info(f"Результат записан в {output_file}")
        console_info(f"Готово. Результат сохранен: {output_file}")
        
    except Exception as e:
        logger.exception(f"Ошибка при выполнении job: {e}")
        console_info(f"Ошибка: {e}")
        raise

if __name__ == "__main__":
    run()
