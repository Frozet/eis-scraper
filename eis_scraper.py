import requests
from bs4 import BeautifulSoup
import xmltodict
from celery import Celery, Task, group
import time
import random
from tenacity import retry, stop_after_attempt, wait_exponential

# Инициализация Celery с использованием Redis в качестве брокера и бэкенда
app = Celery('eis_scraper', broker='redis://redis:6379/0')
app.conf.update(result_backend='redis://redis:6379/0')  # Хранение результатов в Redis
app.conf.broker_connection_retry_on_startup = True  # Повторные попытки подключения при старте

# Базовый URL и список страниц для парсинга
BASE_URL = "https://zakupki.gov.ru"
PAGES = [
    f"{BASE_URL}/epz/order/extendedsearch/results.html?fz44=on&pageNumber={i}" for i in range(1, 3)
]

class ScraperTask(Task):
    # Заголовки для HTTP-запросов, имитирующие браузер
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # Повторные попытки с экспоненциальной задержкой в случае ошибок
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def run(self, page_url):
        """Сбор ссылок на XML-документы с указанной страницы."""
        print(f"Начало сбора ссылок со страницы: {page_url}")
        response = requests.get(page_url, headers=self.HEADERS, timeout=30)
        response.raise_for_status()  # Проверка на ошибки HTTP
        soup = BeautifulSoup(response.text, 'html.parser')  # Парсинг HTML
        time.sleep(random.uniform(1, 3))  # Случайная задержка для избежания блокировки

        # Поиск ссылок на XML-документы
        links = []
        for item in soup.select('.registry-entry__header-mid__number a'):
            tender_link = item['href']
            reg_number = tender_link.split('regNumber=')[-1]
            xml_link = f"{BASE_URL}/epz/order/notice/printForm/viewXml.html?regNumber={reg_number}"
            links.append(xml_link)

        print(f"Ссылки собраны")
        return links

class XMLParserTask(Task):
    # Заголовки для HTTP-запросов
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # Повторные попытки с экспоненциальной задержкой в случае ошибок
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def fetch_xml(self, xml_link):
        """Загрузка XML-документа по ссылке."""
        try:
            response = requests.get(xml_link, headers=self.HEADERS, timeout=30)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            raise  # Повторная попытка

    def find_publish_date(self, data):
        """Рекурсивный поиск даты публикации в XML-документе."""
        if isinstance(data, dict):
            for key, value in data.items():
                if key == 'publishDTInEIS':
                    return value
                result = self.find_publish_date(value)
                if result is not None:
                    return result
        elif isinstance(data, list):
            for item in data:
                result = self.find_publish_date(item)
                if result is not None:
                    return result
        return None

    def run(self, xml_link):
        """Обработка XML-ссылки и извлечение даты публикации."""
        try:
            xml_content = self.fetch_xml(xml_link)
            if xml_content:
                xml_dict = xmltodict.parse(xml_content)  # Парсинг XML в словарь
                publish_date = self.find_publish_date(xml_dict)  # Поиск даты публикации
                xml_link = xml_link.replace('Xml', '')
                return f"✅ {xml_link} - {publish_date}"
        except Exception as e:
            return f"❌ {xml_link} - Ошибка при обработке XML: {e}"

# Регистрация задач в Celery
@app.task(base=ScraperTask)
def scrape_page(page_url):
    return ScraperTask().run(page_url)

@app.task(base=XMLParserTask)
def parse_xml(xml_link):
    return XMLParserTask().run(xml_link)

def main():
    """Основная функция для запуска парсинга."""
    links = []
    for page in PAGES:
        result = scrape_page.delay(page)  # Асинхронный запуск задачи
        links.extend(result.get())  # Сбор ссылок

    # Групповой запуск задач для парсинга XML
    task_group = group(parse_xml.s(link) for link in links).apply_async()

    print("Ожидание выполнения задач...")
    while not task_group.ready():  # Ожидание завершения всех задач
        time.sleep(1)

    print("Все задачи выполнены.")

if __name__ == "__main__":
    main()