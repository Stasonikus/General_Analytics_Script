import logging
import time

# Логгер для информационных сообщений
info_logger = logging.getLogger('info_logger')
info_logger.setLevel(logging.INFO)
info_handler = logging.FileHandler(
    'log_book_info.log', mode='w')  # Перезаписывать файл
info_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s'))
info_logger.addHandler(info_handler)

# Логгер для ошибок
error_logger = logging.getLogger('error_logger')
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(
    'log_book_err.log', mode='w')  # Перезаписывать файл
error_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s'))
error_logger.addHandler(error_handler)

# Логгер для предупреждений
warning_logger = logging.getLogger('warning_logger')
warning_logger.setLevel(logging.WARNING)
warning_handler = logging.FileHandler(
    'log_book_warn.log', mode='w')  # Перезаписывать файл
warning_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s'))
warning_logger.addHandler(warning_handler)

# Логгер для общего отчёта
summary_logger = logging.getLogger('summary_logger')
summary_logger.setLevel(logging.INFO)
summary_handler = logging.FileHandler('log_book_summary.log', mode='w')
summary_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
summary_logger.addHandler(summary_handler)

# Основной логгер
module_logger = logging.getLogger('main_logger')
module_logger.setLevel(logging.INFO)
module_handler = logging.FileHandler(
    'log_book_info.log', mode='w')  # Перезаписывать файл
module_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s'))
module_logger.addHandler(module_handler)

# Новый логгер для отчёта о выполнении


def log_summary(start_time, end_time, books_found, errors_count, warnings_count):
    duration = end_time - start_time
    summary_logger.info(f"Программа начала выполнение в: {start_time}")
    summary_logger.info(f"Программа завершила выполнение в: {end_time}")
    summary_logger.info(f"Продолжительность выполнения: {duration:.2f} секунд")
    summary_logger.info(f"Количество найденных книг: {books_found}")
    summary_logger.info(f"Количество ошибок: {errors_count}")
    summary_logger.info(f"Количество предупреждений: {warnings_count}")


# Создание логгера для тестов переходов между окнами
test_logger = logging.getLogger("test_logger")
test_logger.setLevel(logging.INFO)

# Обработчик для записи в отдельный файл
test_handler = logging.FileHandler("log_test.log", mode="w", encoding="utf-8")
test_formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
test_handler.setFormatter(test_formatter)
test_logger.addHandler(test_handler)

# Функции для подсчета строк в лог-файлах


def count_log_entries(file_path):
    """
    Подсчитывает количество строк в указанном лог-файле.
    """
    try:
        with open(file_path, 'r') as file:
            return sum(1 for _ in file)
    except FileNotFoundError:
        return 0


def count_error_logs():
    return count_log_entries('log_book_err.log')


def count_warning_logs():
    return count_log_entries('log_book_warn.log')
