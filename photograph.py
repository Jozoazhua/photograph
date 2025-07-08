import logging
import os
import platform
import random
import re
import sys
import time
from collections import deque
from functools import partial
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from PySide6.QtCore import (QObject, QPoint, QRect, QSize, Qt, QThread, QTimer,
                            QUrl, Signal)
from PySide6.QtGui import (QColor, QDesktopServices, QFont, QPainter, QPen,
                           QPixmap, QResizeEvent)
from PySide6.QtWidgets import (QApplication, QComboBox, QDialog, QFileDialog,
                               QFrame, QGridLayout, QHBoxLayout, QLabel,
                               QLineEdit, QMessageBox, QProgressBar,
                               QProgressDialog, QPushButton, QScrollArea,
                               QSizePolicy, QSpacerItem, QStatusBar,
                               QTabWidget, QTreeWidget, QTreeWidgetItem,
                               QVBoxLayout, QWidget)

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Constants & Styles ---
BASE_URL = "https://www.mxd009.cc"
GALLERY = "/gallery/"
RANKINGS = {
    "请选择排行榜": "",
    "点击排行": "/sort/onclick/",
    "收藏排行": "/sort/favnum/",
    "点赞排行": "/sort/diggtop/",
    "下载排行": "/sort/totaldown/"
}
TAGS = {
    "请选择标签": "",
    "丝袜诱惑": "/tags/siwayouhuo.html",
    "丝袜美腿": "/tags/siwameitui.html",
    "萝莉控": "/tags/luolikong.html",
    "黑丝诱惑": "/tags/heisiyouhuo.html",
    "街拍": "/tags/jiepai.html",
    "美臀": "/tags/meitun.html",
    "大尺-度": "/tags/dachi-du.html",
    "JK": "/tags/jk.html",
    "COS": "/tags/cos.html",
    "美胸": "/tags/meixiong.html",
    "制服诱惑": "/tags/zhifuyouhuo.html",
    "私房": "/tags/sifang.html",
    "性感": "/tags/xinggan.html",
    "丝足诱惑": "/tags/sizuyouhuo.html",
    "尤物": "/tags/youwu.html"
}
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 OPR/107.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Safari/605.1.15"
]
DEFAULT_HEADERS = {
    "User-Agent": random.choice(USER_AGENTS),
    "Referer": BASE_URL,
}
MAX_CONCURRENT_DOWNLOADS = 3


def get_download_directory() -> str:
    """Returns the default download directory based on the operating system."""
    if platform.system() == "Darwin":  # macOS
        return os.path.expanduser("~/Downloads")
    return os.getcwd()


# --- Core Web Scraping Logic ---
class WebScraper:
    """A collection of static methods for scraping the website."""

    @staticmethod
    def get_session() -> requests.Session:
        """Creates and configures a requests Session object."""
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)
        return session

    @staticmethod
    def construct_page_url(base_url: str, page: int) -> str:
        """
        Constructs the correct URL for a given page number based on the base URL format.

        Args:
            base_url: The base URL of the search result or category page.
            page: The desired page number (1-indexed).

        Returns:
            The fully constructed URL for the specified page.
        """
        if "searchid" in base_url and (match := re.search(r"searchid=(\d+)", base_url)):
            # Search result pagination (uses 0-indexed page query param)
            return f"{BASE_URL}/e/search/result/index.php?page={page - 1}&searchid={match.group(1)}"

        if page == 1:
            return base_url

        path, ext = os.path.splitext(base_url)
        path = re.sub(r'(_\d+)$', '', path)

        if "/tags/" in base_url:
            return f"{path}_{page}{ext}"

        if base_url.endswith('/'):
            base = base_url.rstrip('/')
            return f"{base}/index_{page}.html"

        return f"{path}_{page}{ext}"

    @staticmethod
    def submit_search(session: requests.Session, keywords: str) -> Optional[str]:
        """Submits a search query and returns the result URL."""
        form_data = {
            "keyboard": keywords,
            "show": "title",
            "tempid": "1",
            "tbname": "news"
        }
        search_url = f"{BASE_URL}/e/search/index.php"
        try:
            response = session.post(
                search_url, data=form_data, allow_redirects=False, timeout=15
            )
            if response.status_code == 302:
                return urljoin(search_url, response.headers.get("Location"))
            return None
        except requests.RequestException as e:
            logger.error(f"Search request failed: {e}")
            return None

    @staticmethod
    def parse_search_results_page(html_content: str) -> Tuple[List[Dict[str, str]], int]:
        """Parses a page of gallery listings and extracts item data and total page count."""
        soup = BeautifulSoup(html_content, "html.parser")
        items = []
        total_pages = 1

        if gallery_root := soup.find("div", class_="box galleryList"):
            for li in gallery_root.select("ul.databox > li"):
                img_tag = li.select_one("div.img-box img")
                ztitle_tag = li.select_one("p.ztitle a")
                rtitle_tag = li.select_one("p.rtitle a")
                author_tag = li.select_one("p.ztitle font")
                count_tag = li.select_one("em.num")

                count_text = count_tag.get_text(strip=True) if count_tag else ""
                count_match = re.search(r'\d+', count_text)

                items.append({
                    "img": img_tag["src"] if img_tag and img_tag.has_attr('src') else "",
                    "ztitle": ztitle_tag.get_text(strip=True) if ztitle_tag else "N/A",
                    "ztitle_href": urljoin(BASE_URL, ztitle_tag["href"]) if ztitle_tag else "",
                    "author": (author_tag.get_text(strip=True) if author_tag else
                               (rtitle_tag.get_text(strip=True) if rtitle_tag else "N/A")),
                    "rtitle": rtitle_tag.get_text(strip=True) if rtitle_tag else "N/A",
                    "count": count_match.group(0) if count_match else "0",
                })

        # Logic to find total pages from different pagination styles
        page_div = soup.find("div", id="page") or soup.find("div", class_="layui-box layui-laypage")
        if page_div:
            page_info = page_div.find("span", class_="layui-laypage-count") or page_div.find("span")
            if page_info:
                if match := re.search(r'(\d+)/(\d+)', page_info.text):
                    total_pages = int(match.group(2))
                elif match := re.search(r'/(\d+)', page_info.text):
                    total_pages = int(match.group(1))
        elif (tag_div := soup.find("div", class_="biaoqian")) and (p_text := tag_div.find("p")):
            if match := re.search(r'(\d+)', p_text.get_text(strip=True)):
                total_pages = (int(match.group(1)) + 19) // 20  # 20 items per page
        return items, total_pages

    @staticmethod
    def crawl_single_gallery(session: requests.Session, url: str) -> List[Dict[str, str]]:
        """Crawls a single gallery page, typically from a direct URL paste."""
        try:
            response = session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            title_tag = soup.select_one('div.gallery_jieshao h1')
            img_tag = soup.select_one('div.gallerypic img')
            type_author = [a.get_text(strip=True) for a in soup.select('.gallery_renwu_title a')]

            total_count = 0
            if tishi_div := soup.find('div', id='tishi'):
                if p_tag := tishi_div.find('p'):
                    if match := re.search(r'全本(\d+)张图片', p_tag.get_text()):
                        total_count = int(match.group(1))

            if total_count == 0 and (page_div := soup.find('div', id='page')):
                if span := page_div.find('span', string=re.compile(r'\d+/\d+')):
                    if match := re.search(r'\d+/(\d+)', span.text):
                        total_count = int(match.group(1))

            return [{
                "img": img_tag['src'] if img_tag else "",
                "ztitle": title_tag.get_text(strip=True) if title_tag else "N/A",
                "ztitle_href": url,
                "author": type_author[1] if len(type_author) > 1 else "N/A",
                "rtitle": type_author[0] if type_author else "N/A",
                "count": str(total_count)
            }]
        except Exception as e:
            logger.error(f"Failed to crawl single gallery at {url}: {e}")
            return []


# --- Worker Threads ---
class BaseWorker(QThread):
    """Base worker thread with cancellation support."""
    error = Signal(str)

    def __init__(self):
        super().__init__()
        self.is_cancelled = False

    def cancel(self):
        """Flags the worker to be cancelled."""
        self.is_cancelled = True


class ImageLoadWorker(BaseWorker):
    """Worker to load a single image for thumbnails."""
    image_loaded = Signal(QPixmap)
    load_failed = Signal()

    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def run(self):
        if self.is_cancelled:
            return
        session = WebScraper.get_session()
        try:
            response = session.get(self.url, timeout=15)
            response.raise_for_status()
            pixmap = QPixmap()
            if pixmap.loadFromData(response.content):
                self.image_loaded.emit(pixmap)
            else:
                self.load_failed.emit()
        except Exception:
            if not self.is_cancelled:
                self.load_failed.emit()


class SearchWorker(BaseWorker):
    """Worker to perform a keyword search."""
    results_ready = Signal(list, int, str)

    def __init__(self, query: str):
        super().__init__()
        self.query = query

    def run(self):
        session = WebScraper.get_session()
        try:
            url = WebScraper.submit_search(session, self.query)
            if not url or self.is_cancelled:
                return self.error.emit("搜索失败或已取消。")

            response = session.get(url, timeout=15)
            if self.is_cancelled:
                return

            items, pages = WebScraper.parse_search_results_page(response.text)
            if not items:
                return self.error.emit("没有找到结果。")

            self.results_ready.emit(items, pages, url)
        except Exception as e:
            if not self.is_cancelled:
                self.error.emit(f"搜索错误: {e}")


class PageFetchWorker(BaseWorker):
    """Worker to fetch a single page of results."""
    results_ready = Signal(list, int)

    def __init__(self, base_url: str, page: int):
        super().__init__()
        self.base_url, self.page = base_url, page

    def run(self):
        session = WebScraper.get_session()
        page_url = ""
        try:
            page_url = WebScraper.construct_page_url(self.base_url, self.page)
            if self.is_cancelled:
                return

            response = session.get(page_url, timeout=15)
            response.raise_for_status()
            if self.is_cancelled:
                return

            items, pages = WebScraper.parse_search_results_page(response.text)
            self.results_ready.emit(items, pages)
        except Exception as e:
            if not self.is_cancelled:
                self.error.emit(f"翻页错误 (Page {self.page}, URL: {page_url}): {e}")


class AllPagesFetchWorker(BaseWorker):
    """Worker to fetch all pages of results for a given query."""
    progress = Signal(int, int)
    results_ready = Signal(list)

    def __init__(self, base_url: str, total_pages: int):
        super().__init__()
        self.base_url, self.total_pages = base_url, total_pages

    def run(self):
        session = WebScraper.get_session()
        all_items = []
        page_url = ""
        try:
            for page in range(1, self.total_pages + 1):
                if self.is_cancelled:
                    return self.error.emit("已取消")

                self.progress.emit(page, self.total_pages)
                page_url = WebScraper.construct_page_url(self.base_url, page)
                response = session.get(page_url, timeout=15)
                items, _ = WebScraper.parse_search_results_page(response.text)
                all_items.extend(items)
                time.sleep(0.1)  # Be polite to the server

            self.results_ready.emit(all_items)
        except Exception as e:
            if not self.is_cancelled:
                self.error.emit(f"获取全部页面时出错 (Page {page}, URL: {page_url}): {e}")


class GalleryWorker(BaseWorker):
    """Worker to fetch info for a single gallery from a direct URL."""
    results_ready = Signal(list)

    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def run(self):
        session = WebScraper.get_session()
        try:
            if self.is_cancelled:
                return
            self.results_ready.emit(WebScraper.crawl_single_gallery(session, self.url))
        except Exception as e:
            if not self.is_cancelled:
                self.error.emit(f"获取图集信息错误: {e}")


class ThumbnailWorker(BaseWorker):
    """Worker to determine the URLs for all images in a gallery."""
    urls_ready = Signal(list)

    def __init__(self, url: str, count: int, callback: Optional[callable] = None):
        super().__init__()
        self.url, self.count, self.callback = url, count, callback

    def run(self):
        session = WebScraper.get_session()
        try:
            if self.is_cancelled:
                return

            response = session.get(self.url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            tag = soup.select_one("div.gallerypic img")
            src = tag.get("src") if tag else None

            if not src:
                urls = []
            elif not (match := re.search(r'/(\d+)(\.\w+)$', src)):
                urls = [src]  # Single image gallery
            else:
                num_str, ext = match.groups()
                start_num, padding = int(num_str), len(num_str)
                base = urljoin(self.url, src.rsplit('/', 1)[0])
                urls = [f"{base}/{i:0{padding}d}{ext}" for i in range(start_num, start_num + self.count)]

            if self.callback:
                self.callback(urls)
            else:
                self.urls_ready.emit(urls)

        except Exception as e:
            if not self.is_cancelled:
                self.error.emit(f"获取缩略图网址错误: {e}")


class DownloadWorker(BaseWorker):
    """Worker to download a complete image gallery."""
    progress = Signal(QTreeWidgetItem, int)
    finished = Signal(QTreeWidgetItem, str, bool)

    def __init__(self, task_item: QTreeWidgetItem, download_dir: str):
        super().__init__()
        self.task_item = task_item
        data = task_item.data(0, Qt.ItemDataRole.UserRole)
        self.author = task_item.text(3)
        self.title = task_item.text(0)
        self.url = data["url"]
        self.total_count = data["count"]
        self.download_dir = download_dir
        self.is_paused = False

    def pause(self):
        """Pauses the download."""
        self.is_paused = True
        if self.task_item.text(1) == "下载中":
            self.task_item.setText(1, "已暂停")

    def resume(self):
        """Resumes a paused download."""
        self.is_paused = False
        if self.task_item.text(1) == "已暂停":
            self.task_item.setText(1, "下载中")

    def run(self):
        result_message = ""
        is_success = False
        try:
            self.session = WebScraper.get_session()
            urls = self._get_image_urls()
            if not urls:
                result_message = "链接获取失败"
                is_success = False
                return

            safe_author = re.sub(r'[\\/*?:"<>|]', "", self.author)
            safe_title = re.sub(r'[\\/*?:"<>|]', "", self.title)
            album_dir = os.path.join(self.download_dir, safe_author, safe_title)
            os.makedirs(album_dir, exist_ok=True)

            total, success_count = len(urls), 0
            for i, url in enumerate(urls):
                while self.is_paused:
                    if self.is_cancelled:
                        result_message = "已取消"
                        is_success = False
                        return
                    time.sleep(0.5)

                if self.is_cancelled:
                    result_message = "已取消"
                    is_success = False
                    return

                ext = os.path.splitext(urlparse(url).path)[1] or ".jpg"
                filename = os.path.join(album_dir, f"{i + 1:04d}{ext}")
                if self._download_image(url, filename):
                    success_count += 1

                self.progress.emit(self.task_item, int((i + 1) / total * 100))

            result_message = f"完成 ({success_count}/{total})"
            is_success = True

        except Exception as e:
            if not self.is_cancelled:
                result_message = f"错误: {e}"
            else:
                result_message = "已取消"
            is_success = False
        finally:
            self.finished.emit(self.task_item, result_message, is_success)

    def _get_image_urls(self) -> List[str]:
        response = self.session.get(self.url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        tag = soup.select_one("div.gallerypic img")
        src = tag.get("src") if tag else None
        if not src:
            return []
        if not (match := re.search(r'/(\d+)(\.\w+)$', src)):
            return [src]

        num_str, ext = match.groups()
        start_num, padding = int(num_str), len(num_str)
        base = urljoin(self.url, src.rsplit('/', 1)[0])
        return [f"{base}/{i:0{padding}d}{ext}" for i in range(start_num, start_num + self.total_count)]

    def _download_image(self, url: str, filepath: str) -> bool:
        if os.path.exists(filepath):
            return True
        try:
            with self.session.get(url, timeout=20, stream=True) as r:
                r.raise_for_status()
                with open(filepath, "wb") as f:
                    for chunk in r.iter_content(8192):
                        while self.is_paused:
                            if self.is_cancelled:
                                return False
                            time.sleep(0.5)
                        if self.is_cancelled:
                            return False
                        f.write(chunk)
            return True
        except requests.RequestException:
            return False


class FileDownloadWorker(QThread):
    """Worker for downloading a single file with progress."""
    progress = Signal(int)
    finished = Signal(bool, str)

    def __init__(self, url: str, filepath: str, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.url, self.filepath = url, filepath
        self.is_cancelled = False

    def run(self):
        session = WebScraper.get_session()
        try:
            with session.get(self.url, timeout=30, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                bytes_downloaded = 0
                with open(self.filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if self.is_cancelled:
                            return self.finished.emit(False, "已取消")
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
                        if total_size > 0:
                            self.progress.emit(int(bytes_downloaded * 100 / total_size))
            self.finished.emit(True, "下载完成。")
        except Exception as e:
            if not self.is_cancelled:
                self.finished.emit(False, str(e))

    def cancel(self):
        self.is_cancelled = True


# --- Managers ---
class ImageDownloadManager(QObject):
    """Manages concurrent thumbnail image loading."""

    def __init__(self, max_concurrent=10):
        super().__init__()
        self.max_concurrent = max_concurrent
        self.active_workers: Dict[ImageLoadWorker, Dict] = {}
        self.pending_requests: deque = deque()
        self.is_shutting_down = False

    def download_image(self, url, owner, callback, error_callback=None):
        if self.is_shutting_down:
            return
        request = {
            'url': url,
            'owner': owner,
            'callback': callback,
            'error_callback': error_callback or (lambda: None)
        }
        if len(self.active_workers) < self.max_concurrent:
            self._start_download(request)
        else:
            self.pending_requests.append(request)

    def _start_download(self, request: Dict):
        worker = ImageLoadWorker(request['url'])
        worker.image_loaded.connect(request['callback'])
        worker.load_failed.connect(request['error_callback'])
        worker.finished.connect(partial(self._on_worker_finished, worker))
        worker.finished.connect(worker.deleteLater)
        self.active_workers[worker] = request
        worker.start()

    def _on_worker_finished(self, worker: ImageLoadWorker):
        if worker in self.active_workers:
            del self.active_workers[worker]
        if self.pending_requests and not self.is_shutting_down:
            self._start_download(self.pending_requests.popleft())

    def cancel_requests_for_owner(self, owner: QObject):
        """Cancel all requests initiated by a specific owner widget."""
        self.pending_requests = deque(
            [r for r in self.pending_requests if r['owner'] != owner]
        )
        for worker, req in list(self.active_workers.items()):
            if req['owner'] == owner:
                worker.cancel()

    def shutdown(self) -> List[QThread]:
        """Cancels all workers and prepares for application exit."""
        logger.info("Shutting down ImageDownloadManager...")
        self.is_shutting_down = True
        self.pending_requests.clear()
        for worker in list(self.active_workers.keys()):
            worker.cancel()
        return list(self.active_workers.keys())


class APIManager(QObject):
    """Manages API worker threads to prevent concurrent searches."""
    search_results_ready = Signal(list, int, str)
    page_results_ready = Signal(list, int)
    gallery_info_ready = Signal(list)
    thumbnail_urls_ready = Signal(list)
    error = Signal(str)
    all_pages_results_ready = Signal(list)
    all_pages_progress = Signal(int, int)

    def __init__(self):
        super().__init__()
        self.current_worker: Optional[BaseWorker] = None
        self.concurrent_workers: List[BaseWorker] = []

    def _start_worker(self, worker_class, *args, concurrent: bool = False):
        if not concurrent and self.current_worker and self.current_worker.isRunning():
            logger.info(f"Cancelling previous worker: {type(self.current_worker).__name__}")
            self.current_worker.cancel()
            self.current_worker.quit()
            self.current_worker.wait(500)

        worker = worker_class(*args)

        if not concurrent:
            self.current_worker = worker
            worker.finished.connect(lambda: self._clear_worker_ref(worker))
        else:
            self.concurrent_workers.append(worker)
            worker.finished.connect(lambda: self._remove_concurrent_worker(worker))

        # Connect signals
        worker.error.connect(self.error)
        worker.finished.connect(worker.deleteLater)
        if isinstance(worker, SearchWorker):
            worker.results_ready.connect(self.search_results_ready)
        elif isinstance(worker, PageFetchWorker):
            worker.results_ready.connect(self.page_results_ready)
        elif isinstance(worker, GalleryWorker):
            worker.results_ready.connect(self.gallery_info_ready)
        elif isinstance(worker, ThumbnailWorker):
            # QueuedConnection is important for inter-thread signal safety
            worker.urls_ready.connect(self.thumbnail_urls_ready, Qt.ConnectionType.QueuedConnection)
        elif isinstance(worker, AllPagesFetchWorker):
            worker.results_ready.connect(self.all_pages_results_ready)
            worker.progress.connect(self.all_pages_progress)

        worker.start()

    def _remove_concurrent_worker(self, worker: BaseWorker):
        if worker in self.concurrent_workers:
            self.concurrent_workers.remove(worker)

    def _clear_worker_ref(self, worker: BaseWorker):
        if self.current_worker is worker:
            self.current_worker = None

    def search(self, query: str):
        if query.startswith(f"{BASE_URL}/gallery"):
            self._start_worker(GalleryWorker, query)
        elif query.startswith(BASE_URL):
            self._start_worker(PageFetchWorker, query, 1)
        else:
            self._start_worker(SearchWorker, query)

    def fetch_page(self, base_url: str, page: int):
        self._start_worker(PageFetchWorker, base_url, page)

    def fetch_all_pages(self, base_url: str, total_pages: int):
        self._start_worker(AllPagesFetchWorker, base_url, total_pages)

    def fetch_thumbnail_urls(self, url: str, count: int):
        # Thumbnail workers can run concurrently as they are lightweight
        self._start_worker(ThumbnailWorker, url, count, concurrent=True)

    def cancel_all(self):
        if self.current_worker and self.current_worker.isRunning():
            self.current_worker.cancel()

    def shutdown(self) -> List[QThread]:
        """Cancels all running workers."""
        self.cancel_all()
        all_workers = []
        if self.current_worker and self.current_worker.isRunning():
            all_workers.append(self.current_worker)
        all_workers.extend([w for w in self.concurrent_workers if w.isRunning()])

        for worker in all_workers:
            worker.cancel()
        return all_workers


# --- UI Components ---
class ThumbnailWidget(QFrame):
    """A widget to display a single gallery thumbnail and its info."""
    STATUS_PENDING, STATUS_LOADING, STATUS_LOADED, STATUS_FAILED = range(4)

    def __init__(self, item_data: Dict[str, Any], parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.item_data = item_data
        self._is_selected = False
        self.load_status = self.STATUS_PENDING
        self.setFrameShape(self.Shape.StyledPanel)
        self.setFrameShadow(self.Shadow.Raised)
        self.setFixedSize(220, 360)
        self.setStyleSheet("QFrame { border: 1px solid #ddd; border-radius: 5px; background-color: white; }")
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)
        title_font = QFont()
        title_font.setBold(True)
        title_label = QLabel(f"<a href='{self.item_data['ztitle_href']}'>{self.item_data['ztitle']}</a>")
        title_label.setFont(title_font)
        title_label.setWordWrap(True)
        title_label.setOpenExternalLinks(True)
        title_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextBrowserInteraction)
        self.img_label = QLabel("等待加载…")
        self.img_label.setFixedSize(200, 250)
        self.img_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.img_label.setStyleSheet("border: 1px solid #eee; background-color: #f8f8f8; border-radius: 3px;")
        info_label = QLabel(f"{self.item_data['author']} ({self.item_data['count']}P)")
        info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        info_label.setStyleSheet("color: #666;")
        btn_layout = QHBoxLayout()
        self.preview_btn = QPushButton("预览图")
        self.original_image_btn = QPushButton("原图集")
        btn_layout.addWidget(self.preview_btn)
        btn_layout.addWidget(self.original_image_btn)
        layout.addWidget(title_label)
        layout.addWidget(self.img_label)
        layout.addWidget(info_label)
        layout.addLayout(btn_layout)

    def isSelected(self) -> bool:
        return self._is_selected

    def setSelected(self, selected: bool):
        if self._is_selected != selected:
            self._is_selected = selected
            self.update()  # Trigger a repaint

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.setSelected(not self._is_selected)
        super().mousePressEvent(event)

    def paintEvent(self, event):
        super().paintEvent(event)
        if self._is_selected:
            painter = QPainter(self)
            pen = QPen(QColor("#4F86F7"), 3)
            painter.setPen(pen)
            painter.drawRect(self.rect().adjusted(1, 1, -1, -1))


class OriginalImageViewer(QDialog):
    """A dialog for viewing full-resolution images with lazy loading."""

    def __init__(self, image_urls: List[str], title: str, count: int, parent: QWidget,
                 image_manager: ImageDownloadManager):
        super().__init__(parent)
        self.image_manager = image_manager
        self.urls = image_urls
        self.image_widgets: List[Dict] = []
        self.is_closing = False

        self.setWindowTitle(f"原图查看 (共 {count} 张) - {title}")
        self.setWindowState(Qt.WindowState.WindowMaximized)

        self.scroll_area = QScrollArea(self)
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setStyleSheet("QScrollArea { border: none; background-color: #333; }")

        container = QWidget()
        container.setStyleSheet("background-color: #333;")
        self.scroll_area.setWidget(container)

        self.layout = QVBoxLayout(container)
        self.layout.setSpacing(20)
        self.layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        dialog_layout = QVBoxLayout(self)
        dialog_layout.setContentsMargins(0, 0, 0, 0)
        dialog_layout.addWidget(self.scroll_area)

        self._populate_layout()

        self.scroll_area.verticalScrollBar().valueChanged.connect(self._check_visible_and_load)
        QTimer.singleShot(100, self._check_visible_and_load)

    def _populate_layout(self):
        for i, url in enumerate(self.urls):
            image_label = QLabel(f"图片 {i + 1} 加载中...")
            image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            image_label.setMinimumSize(200, 200)
            image_label.setStyleSheet("color: white; border: 1px dashed #888; border-radius: 5px;")
            self.layout.addWidget(image_label)
            self.image_widgets.append({
                'widget': image_label,
                'url': url,
                'status': ThumbnailWidget.STATUS_PENDING
            })

    def _check_visible_and_load(self):
        if self.is_closing:
            return
        scrollbar = self.scroll_area.verticalScrollBar()
        viewport = self.scroll_area.viewport()
        visible_rect = QRect(0, scrollbar.value(), viewport.width(), viewport.height())

        for item in self.image_widgets:
            if item['status'] == ThumbnailWidget.STATUS_PENDING:
                widget = item['widget']
                if visible_rect.intersects(widget.geometry()):
                    item['status'] = ThumbnailWidget.STATUS_LOADING
                    self.image_manager.download_image(
                        item['url'], self,
                        partial(self.set_image, item),
                        partial(self.set_image_error, item)
                    )

    def set_image(self, item: Dict, pixmap: QPixmap):
        if self.is_closing:
            return
        try:
            widget = item['widget']
            item['status'] = ThumbnailWidget.STATUS_LOADED

            window_width = self.scroll_area.width() - 40  # Account for scrollbar/margins
            scaled_pixmap = pixmap.scaledToWidth(window_width, Qt.TransformationMode.SmoothTransformation)
            widget.setPixmap(scaled_pixmap)
            widget.setFixedSize(scaled_pixmap.size())
            widget.setStyleSheet("border: none;")

        except RuntimeError:  # Widget might have been deleted
            pass

    def set_image_error(self, item: Dict):
        if self.is_closing:
            return
        try:
            item['status'] = ThumbnailWidget.STATUS_FAILED
            item['widget'].setText("图片加载失败")
        except RuntimeError:
            pass

    def closeEvent(self, event):
        self.is_closing = True
        self.image_manager.cancel_requests_for_owner(self)
        super().closeEvent(event)

    def keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_Escape:
            self.close()


class PreviewItemWidget(QFrame):
    """A single item in the thumbnail preview dialog."""
    view_original_clicked = Signal(str)
    download_original_clicked = Signal(str)

    def __init__(self, url: str, serial: str, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.url = url
        self.setFrameShape(self.Shape.StyledPanel)
        self.setFixedSize(180, 320)
        self.thumb_size = QSize(170, 240)
        v_layout = QVBoxLayout(self)
        v_layout.setContentsMargins(5, 5, 5, 5)
        v_layout.setSpacing(2)
        num_label = QLabel(serial)
        num_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        num_label.setMaximumHeight(10)
        self.image_label = QLabel("等待加载…")
        self.image_label.setFixedSize(self.thumb_size)
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image_label.setStyleSheet("border: 1px solid gray; background-color: #f0f0f0;")
        btn_layout = QHBoxLayout()
        view_btn = QPushButton("查看原图")
        download_btn = QPushButton("原图下载")
        view_btn.setMaximumWidth(80)
        download_btn.setMaximumWidth(80)
        view_btn.clicked.connect(lambda: self.view_original_clicked.emit(self.url))
        download_btn.clicked.connect(lambda: self.download_original_clicked.emit(self.url))
        btn_layout.addItem(QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum))
        btn_layout.addWidget(view_btn)
        btn_layout.addWidget(download_btn)
        btn_layout.addItem(QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum))
        v_layout.addWidget(num_label)
        v_layout.addWidget(self.image_label)
        v_layout.addLayout(btn_layout)

    def set_loading(self):
        self.image_label.setText("加载中...")

    def set_pixmap(self, pixmap: QPixmap):
        scaled = pixmap.scaled(self.thumb_size, Qt.AspectRatioMode.KeepAspectRatio,
                               Qt.TransformationMode.SmoothTransformation)
        self.image_label.setPixmap(scaled)
        self.image_label.setText("")

    def set_error(self):
        self.image_label.setText("加载失败")


class ThumbnailViewerDialog(QDialog):
    """Dialog for previewing all thumbnails in a gallery."""

    def __init__(self, thumbnail_data: List[Dict], parent: QWidget, image_manager: ImageDownloadManager,
                 api_manager: APIManager):
        super().__init__(parent)
        self.image_manager = image_manager
        self.api_manager = api_manager
        self.items_to_load: List[Dict] = []
        self.is_closing = False
        self.single_downloader: Optional[FileDownloadWorker] = None
        self.download_progress_dialog: Optional[QProgressDialog] = None
        self.file_dialog: Optional[QFileDialog] = None
        album_title = thumbnail_data[0]['title'] if thumbnail_data else ""
        self.setWindowTitle(f"图集预览 (共 {len(thumbnail_data)} 张) - {album_title}")
        self.resize(1100, 700)
        self.scroll_area = QScrollArea(self)
        self.scroll_area.setWidgetResizable(True)
        container = QWidget()
        self.scroll_area.setWidget(container)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.scroll_area)
        self.grid = QGridLayout(container)
        self._populate_grid(thumbnail_data)
        self.scroll_area.verticalScrollBar().valueChanged.connect(self._check_visible_and_load)
        QTimer.singleShot(100, self._check_visible_and_load)

    def _populate_grid(self, thumbnail_data: List[Dict]):
        for i, thumb_info in enumerate(thumbnail_data):
            item_widget = PreviewItemWidget(thumb_info['url'], thumb_info['serial'])
            item_widget.view_original_clicked.connect(self._on_view_original)
            item_widget.download_original_clicked.connect(self._on_download_original)
            row, col = divmod(i, 5)
            self.grid.addWidget(item_widget, row, col)
            self.items_to_load.append(
                {'widget': item_widget, 'url': thumb_info['url'], 'status': ThumbnailWidget.STATUS_PENDING})

    def _check_visible_and_load(self):
        if self.is_closing:
            return
        scrollbar = self.scroll_area.verticalScrollBar()
        viewport = self.scroll_area.viewport()
        visible_rect = QRect(0, scrollbar.value(), viewport.width(), viewport.height())
        for item in self.items_to_load:
            if item['status'] != ThumbnailWidget.STATUS_PENDING or not item['url']:
                continue
            widget = item['widget']
            if not visible_rect.intersects(widget.geometry()):
                continue
            item['status'] = ThumbnailWidget.STATUS_LOADING
            widget.set_loading()
            self.image_manager.download_image(item['url'], self, partial(self.set_grid_image, item),
                                              partial(self.set_grid_image_error, item))

    def set_grid_image(self, item, pixmap):
        if self.is_closing:
            return
        try:
            item['status'] = ThumbnailWidget.STATUS_LOADED
            if item['widget'].isVisible():
                item['widget'].set_pixmap(pixmap)
        except RuntimeError:
            pass

    def set_grid_image_error(self, item):
        if self.is_closing:
            return
        try:
            item['status'] = ThumbnailWidget.STATUS_FAILED
            if item['widget'].isVisible():
                item['widget'].set_error()
        except RuntimeError:
            pass

    def resizeEvent(self, event: QResizeEvent):
        super().resizeEvent(event)
        if not hasattr(self, 'resize_timer'):
            self.resize_timer = QTimer(self)
            self.resize_timer.setSingleShot(True)
            self.resize_timer.timeout.connect(self._check_visible_and_load)
        self.resize_timer.start(150)

    def _on_view_original(self, url: str):
        QDesktopServices.openUrl(QUrl(url))

    def _on_download_original(self, url: str):
        if self.single_downloader and self.single_downloader.isRunning():
            return QMessageBox.information(self, "提示", "已有另一个单张图片正在下载中。")
        if self.file_dialog and self.file_dialog.isVisible():
            self.file_dialog.raise_()
            return self.file_dialog.activateWindow()

        self.file_dialog = QFileDialog(self, "保存图片")
        self.file_dialog.setAcceptMode(QFileDialog.AcceptMode.AcceptSave)
        self.file_dialog.setFileMode(QFileDialog.FileMode.AnyFile)
        self.file_dialog.setWindowModality(Qt.WindowModality.NonModal)
        filename = os.path.basename(urlparse(url).path) or "image.jpg"
        default_path = os.path.join(get_download_directory(), filename)
        self.file_dialog.selectFile(default_path)
        self.file_dialog.fileSelected.connect(lambda path: self._start_single_download(url, path))
        self.file_dialog.open()

    def _start_single_download(self, url: str, save_path: str):
        if not save_path:
            return
        try:
            self.download_progress_dialog = QProgressDialog("正在下载...", "取消", 0, 100, self)
            self.download_progress_dialog.setWindowTitle("下载图片")
            self.download_progress_dialog.setWindowModality(Qt.WindowModality.NonModal)
            self.single_downloader = FileDownloadWorker(url, save_path)
            self.single_downloader.progress.connect(self.download_progress_dialog.setValue)
            self.single_downloader.finished.connect(self.on_single_download_finished)
            self.single_downloader.finished.connect(self.single_downloader.deleteLater)
            self.download_progress_dialog.canceled.connect(self.single_downloader.cancel)
            self.single_downloader.start()
            self.download_progress_dialog.open()
        except Exception as e:
            QMessageBox.critical(self, "错误", f"启动下载失败: {e}")

    def on_single_download_finished(self, success: bool, message: str):
        if self.download_progress_dialog:
            self.download_progress_dialog.close()
            self.download_progress_dialog = None

        if success:
            QMessageBox.information(self, "完成", f"图片下载完成。\n路径: {self.single_downloader.filepath}")
        elif message != "已取消":
            QMessageBox.warning(self, "下载失败", f"无法下载图片: {message}")
        self.single_downloader = None

    def closeEvent(self, event):
        self.is_closing = True
        self.image_manager.cancel_requests_for_owner(self)
        if self.single_downloader and self.single_downloader.isRunning():
            self.single_downloader.cancel()
        if self.file_dialog:
            self.file_dialog.close()
        super().closeEvent(event)


class GalleryCrawler(QWidget):
    """The main application window."""

    def __init__(self):
        super().__init__()
        self.api_manager = APIManager()
        self.image_manager = ImageDownloadManager()
        self.download_directory = get_download_directory()

        self.current_page = 1
        self.total_pages = 1
        self.search_base_url = ""

        self.download_task_queue: deque[QTreeWidgetItem] = deque()
        self.active_download_workers: Dict[QTreeWidgetItem, DownloadWorker] = {}
        self.task_map: Dict[str, QTreeWidgetItem] = {}

        self.current_results: List[Dict] = []
        self.thumbnail_widgets: List[ThumbnailWidget] = []
        self.current_thumbnail_viewer: Optional[ThumbnailViewerDialog] = None
        self.is_shutting_down = False

        self.init_ui()
        self.connect_signals()
        self.setWindowTitle("Dream Gallery Crawler")
        self.resize(1300, 800)

    def init_ui(self):
        root_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        search_widget = QWidget()
        self.downloads_widget = QWidget()
        self.tabs.addTab(search_widget, "🔎 浏览与下载")
        self.tabs.addTab(self.downloads_widget, "📥 下载管理 (0)")
        search_tab_layout = QVBoxLayout(search_widget)

        # --- Top Search & Filter Layout ---
        top_layout = QHBoxLayout()
        self.updates_btn = QPushButton("每日更新")
        top_layout.addWidget(self.updates_btn)

        ranking_layout = QHBoxLayout()
        ranking_layout.addWidget(QLabel("排行榜:"))
        self.ranking_combo = QComboBox()
        self.ranking_combo.addItems(RANKINGS.keys())
        self.ranking_combo.model().item(0).setEnabled(False)  # Disable "请选择"
        ranking_layout.addWidget(self.ranking_combo)

        ranking_layout.addWidget(QLabel("标签:"))
        self.tag_combo = QComboBox()
        self.tag_combo.addItems(TAGS.keys())
        self.tag_combo.model().item(0).setEnabled(False)
        ranking_layout.addWidget(self.tag_combo)
        ranking_layout.addStretch()
        top_layout.addLayout(ranking_layout)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("输入关键词搜索或直接粘贴图集网址...")
        self.search_btn = QPushButton("搜索")
        top_layout.addWidget(self.search_input, 1)  # Add stretch factor
        top_layout.addWidget(self.search_btn)

        # --- Main Content Area ---
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setStyleSheet("QScrollArea { border: none; background-color: #f0f0f0; }")
        container = QWidget()
        self.grid_layout = QGridLayout(container)
        self.grid_layout.setSpacing(15)
        container.setStyleSheet("background-color: #f0f0f0;")
        self.scroll_area.setWidget(container)

        # --- Bottom Pagination & Actions Layout ---
        bottom_layout = QHBoxLayout()
        self.btn_download_selected = QPushButton("下载选中")
        self.btn_download_all_on_page = QPushButton("下载本页")
        self.btn_download_all_results = QPushButton("下载全部结果")
        bottom_layout.addWidget(self.btn_download_selected)
        bottom_layout.addWidget(self.btn_download_all_on_page)
        bottom_layout.addWidget(self.btn_download_all_results)
        bottom_layout.addStretch()

        self.prev_btn, self.next_btn = QPushButton("上一页"), QPushButton("下一页")
        self.page_label = QLabel("第 1 / 1 页")
        self.page_input = QLineEdit()
        self.page_input.setPlaceholderText("页码")
        self.page_input.setFixedWidth(50)
        self.go_btn = QPushButton("跳转")
        bottom_layout.addWidget(self.prev_btn)
        bottom_layout.addWidget(self.page_label)
        bottom_layout.addWidget(self.next_btn)
        bottom_layout.addSpacing(10)
        bottom_layout.addWidget(self.page_input)
        bottom_layout.addWidget(self.go_btn)

        # --- Assemble Search Tab ---
        search_tab_layout.addLayout(top_layout)
        search_tab_layout.addWidget(self.scroll_area)
        search_tab_layout.addLayout(bottom_layout)

        # --- Downloads Tab ---
        dl_layout = QVBoxLayout(self.downloads_widget)
        dir_layout = QHBoxLayout()
        dir_layout.addWidget(QLabel("下载目录:"))
        self.dir_path_label = QLabel(self.download_directory)
        self.dir_path_label.setStyleSheet("color: #666;")
        self.select_dir_btn = QPushButton("选择目录")
        self.open_dir_btn = QPushButton("打开目录")
        dir_layout.addWidget(self.dir_path_label, 1)
        dir_layout.addStretch()
        dir_layout.addWidget(self.select_dir_btn)
        dir_layout.addWidget(self.open_dir_btn)
        dl_layout.addLayout(dir_layout)

        self.download_tree = QTreeWidget()
        self.download_tree.setHeaderLabels(["标题", "状态", "进度", "作者", "操作"])
        self.download_tree.setColumnWidth(0, 400)
        self.download_tree.setColumnWidth(1, 120)
        self.download_tree.setColumnWidth(2, 200)
        self.download_tree.setColumnWidth(3, 150)
        self.download_tree.setColumnWidth(4, 150)
        dl_layout.addWidget(self.download_tree)

        dl_btn_layout = QHBoxLayout()
        self.btn_pause_all = QPushButton("全部暂停")
        self.btn_resume_all = QPushButton("全部开始")
        self.btn_cancel_all = QPushButton("全部取消")
        self.btn_clear_finished = QPushButton("清除已完成")
        dl_btn_layout.addStretch()
        dl_btn_layout.addWidget(self.btn_resume_all)
        dl_btn_layout.addWidget(self.btn_pause_all)
        dl_btn_layout.addWidget(self.btn_cancel_all)
        dl_btn_layout.addWidget(self.btn_clear_finished)
        dl_layout.addLayout(dl_btn_layout)

        # --- Status Bar ---
        self.status_bar = QStatusBar()
        root_layout.addWidget(self.tabs)
        root_layout.addWidget(self.status_bar)
        self.set_status_message("就绪")

    def connect_signals(self):
        # Search Tab Signals
        self.updates_btn.clicked.connect(self.fetch_daily_updates)
        self.search_input.returnPressed.connect(self.start_search)
        self.search_btn.clicked.connect(self.start_search)
        self.scroll_area.verticalScrollBar().valueChanged.connect(self._check_visible_and_load)
        self.prev_btn.clicked.connect(self.prev_page)
        self.next_btn.clicked.connect(self.next_page)
        self.go_btn.clicked.connect(self.go_to_page)
        self.page_input.returnPressed.connect(self.go_to_page)
        self.ranking_combo.activated.connect(self.fetch_ranking_updates)
        self.tag_combo.activated.connect(self.fetch_tag_updates)

        # Download Action Signals
        self.btn_download_selected.clicked.connect(self.download_selected)
        self.btn_download_all_on_page.clicked.connect(self.download_all_on_page)
        self.btn_download_all_results.clicked.connect(self.download_all_results)

        # Download Management Signals
        self.btn_pause_all.clicked.connect(self.pause_all_downloads)
        self.btn_resume_all.clicked.connect(self.resume_all_downloads)
        self.btn_cancel_all.clicked.connect(self.cancel_all_downloads)
        self.btn_clear_finished.clicked.connect(self.clear_finished_downloads)
        self.select_dir_btn.clicked.connect(self.select_download_directory)
        self.open_dir_btn.clicked.connect(self.open_download_directory)

        # API Manager Signals
        self.api_manager.search_results_ready.connect(self.on_search_results_ready)
        self.api_manager.page_results_ready.connect(self.on_page_fetch_ready)
        self.api_manager.gallery_info_ready.connect(self.on_single_gallery_ready)
        self.api_manager.thumbnail_urls_ready.connect(self.on_thumbnail_urls_ready)
        self.api_manager.all_pages_results_ready.connect(self.on_all_results_fetched)
        self.api_manager.error.connect(self.on_worker_error)

    def _add_item_actions(self, task_item: QTreeWidgetItem):
        """Adds action buttons (Cancel, Open) to a download task item."""
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(5, 0, 5, 0)
        layout.setSpacing(5)

        cancel_btn = QPushButton("取消")
        cancel_btn.setObjectName("cancel_btn")
        open_dir_btn = QPushButton("打开目录")
        open_dir_btn.setObjectName("open_dir_btn")
        open_dir_btn.hide()

        cancel_btn.clicked.connect(lambda: self._cancel_single_download(task_item))
        open_dir_btn.clicked.connect(lambda: self._open_item_directory(task_item))

        layout.addWidget(cancel_btn)
        layout.addWidget(open_dir_btn)
        layout.addStretch()
        self.download_tree.setItemWidget(task_item, 4, widget)

    def _get_item_action_widget(self, task_item: QTreeWidgetItem, action_name: str) -> Optional[QPushButton]:
        """Finds a specific action button within a download task item."""
        if widget := self.download_tree.itemWidget(task_item, 4):
            return widget.findChild(QPushButton, action_name)
        return None

    def _cancel_single_download(self, item: QTreeWidgetItem):
        """Cancels a single download task."""
        if item in self.active_download_workers:
            self.active_download_workers[item].cancel()
        elif item in self.download_task_queue:
            try:
                self.download_task_queue.remove(item)
            except ValueError:
                pass
            item.setText(1, "已取消")
        self.update_downloads_tab_title()

    def _open_item_directory(self, item: QTreeWidgetItem):
        """Opens the download directory for a specific completed task."""
        author = item.text(3)
        title = item.text(0)
        safe_author = re.sub(r'[\\/*?:"<>|]', "", author)
        safe_title = re.sub(r'[\\/*?:"<>|]', "", title)
        album_dir = os.path.join(self.download_directory, safe_author, safe_title)
        QDesktopServices.openUrl(QUrl.fromLocalFile(album_dir))

    def select_download_directory(self):
        """Opens a dialog to select the download directory."""
        directory = QFileDialog.getExistingDirectory(self, "选择下载目录", self.download_directory)
        if directory:
            self.download_directory = directory
            self.dir_path_label.setText(self.download_directory)
            self.set_status_message(f"下载目录已更新为: {self.download_directory}")

    def open_download_directory(self):
        """Opens the download directory in the file explorer."""
        QDesktopServices.openUrl(QUrl.fromLocalFile(self.download_directory))

    def fetch_tag_updates(self):
        """Fetches updates from the selected tag category."""
        url_path = TAGS.get(self.tag_combo.currentText())
        if not url_path:
            return
        self.set_loading_state(True)
        self.set_status_message(f"正在加载标签: {self.tag_combo.currentText()}...")
        self._clear_grid()
        self.current_page = 1
        self.total_pages = 1
        tag_url = urljoin(BASE_URL, url_path)
        self.search_base_url = tag_url
        self.api_manager.fetch_page(self.search_base_url, self.current_page)

    def fetch_ranking_updates(self):
        """Fetches updates from the selected ranking category."""
        url_path = RANKINGS.get(self.ranking_combo.currentText())
        if not url_path:
            return
        self.set_loading_state(True)
        self.set_status_message(f"正在加载 {self.ranking_combo.currentText()}...")
        self._clear_grid()
        self.current_page = 1
        self.total_pages = 1
        ranking_url = urljoin(BASE_URL, url_path)
        self.search_base_url = ranking_url
        self.api_manager.fetch_page(self.search_base_url, self.current_page)

    def fetch_daily_updates(self):
        """Fetches the first page of the daily updates section."""
        self.set_loading_state(True)
        self.set_status_message("正在加载每日更新...")
        self._clear_grid()
        self.current_page = 1
        self.total_pages = 1
        updates_url = urljoin(BASE_URL, GALLERY)
        self.search_base_url = updates_url
        self.api_manager.fetch_page(self.search_base_url, self.current_page)

    def set_loading_state(self, loading: bool):
        """Enables/disables UI elements during a loading operation."""
        self.search_btn.setEnabled(not loading)
        self.updates_btn.setEnabled(not loading)
        self.scroll_area.setEnabled(not loading)
        self.set_status_message("正在加载..." if loading else "加载完成")

    def set_status_message(self, message: str, timeout: int = 5000):
        self.status_bar.showMessage(message, timeout)

    def update_pagination_controls(self):
        """Updates the state of pagination buttons."""
        is_multi_page = self.total_pages > 1
        self.prev_btn.setEnabled(is_multi_page and self.current_page > 1)
        self.next_btn.setEnabled(is_multi_page and self.current_page < self.total_pages)
        self.page_label.setText(f"第 {self.current_page} / {self.total_pages} 页")
        self.page_input.setEnabled(is_multi_page)
        self.go_btn.setEnabled(is_multi_page)

    def _clear_grid(self):
        """Removes all thumbnail widgets from the grid layout."""
        self.image_manager.cancel_requests_for_owner(self)
        while (item := self.grid_layout.takeAt(0)):
            if widget := item.widget():
                widget.deleteLater()
        self.thumbnail_widgets.clear()

    def _populate_grid(self, results: List[Dict]):
        """Creates and adds thumbnail widgets to the grid."""
        self._clear_grid()
        cols = max(1, (self.scroll_area.width() - 30) // 235)
        for i, item_data in enumerate(results):
            thumb_widget = ThumbnailWidget(item_data)
            thumb_widget.original_image_btn.clicked.connect(partial(self.show_original_images, item_data))
            thumb_widget.preview_btn.clicked.connect(partial(self.show_album_thumbnails, item_data))
            self.thumbnail_widgets.append(thumb_widget)
            row, col = divmod(i, cols)
            self.grid_layout.addWidget(thumb_widget, row, col)
        QTimer.singleShot(100, self._check_visible_and_load)

    def _check_visible_and_load(self):
        """Loads images for visible thumbnail widgets."""
        viewport = self.scroll_area.viewport()
        global_viewport_rect = QRect(viewport.mapToGlobal(QPoint(0, 0)), viewport.size())

        for widget in self.thumbnail_widgets:
            if widget.load_status != ThumbnailWidget.STATUS_PENDING:
                continue
            if not widget.item_data.get('img'):
                continue
            global_widget_rect = QRect(
                widget.mapToGlobal(QPoint(0, 0)), widget.size()
            )
            if not global_viewport_rect.intersects(global_widget_rect):
                continue

            widget.load_status = ThumbnailWidget.STATUS_LOADING
            widget.img_label.setText("加载中...")
            self.image_manager.download_image(
                widget.item_data['img'], self,
                partial(self.set_grid_image, widget),
                partial(self.set_grid_image_error, widget)
            )

    def set_grid_image(self, widget: ThumbnailWidget, pixmap: QPixmap):
        """Slot to set the pixmap for a thumbnail widget."""
        try:
            if widget and widget.isVisible():
                widget.load_status = ThumbnailWidget.STATUS_LOADED
                scaled = pixmap.scaled(
                    widget.img_label.size(),
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation
                )
                widget.img_label.setPixmap(scaled)
                widget.img_label.setText("")
        except RuntimeError:
            pass

    def set_grid_image_error(self, widget: ThumbnailWidget):
        """Slot to handle image loading errors for a thumbnail widget."""
        try:
            if widget and widget.isVisible():
                widget.load_status = ThumbnailWidget.STATUS_FAILED
                widget.img_label.setText("加载失败")
        except RuntimeError:
            pass

    def start_search(self):
        """Initiates a search based on the text in the search input."""
        if not (query := self.search_input.text().strip()):
            return
        self.set_loading_state(True)
        self._clear_grid()
        self.current_page = 1
        self.total_pages = 1
        self.api_manager.search(query)

    def on_search_results_ready(self, items: List[Dict], total_pages: int,
                                base_url: str):
        if self.is_shutting_down:
            return
        self.set_loading_state(False)
        self.total_pages = total_pages
        self.search_base_url = base_url
        self.current_results = items
        self._populate_grid(items)
        self.update_pagination_controls()

    def on_page_fetch_ready(self, items: List[Dict], total_pages: int):
        if self.is_shutting_down:
            return
        self.set_loading_state(False)
        self.total_pages = total_pages
        self.current_results = items
        self._populate_grid(items)
        self.update_pagination_controls()
        self.scroll_area.verticalScrollBar().setValue(0)

    def on_single_gallery_ready(self, results: List[Dict]):
        if self.is_shutting_down:
            return
        self.set_loading_state(False)
        self.total_pages = 1
        self.current_page = 1
        self.current_results = results
        self._populate_grid(results)
        self.update_pagination_controls()

    def on_thumbnail_urls_ready(self, urls: List[str]):
        if self.is_shutting_down:
            return

        try:
            if self.current_thumbnail_viewer:
                self.current_thumbnail_viewer.close()

            if not hasattr(self, 'last_previewed_item'):
                return
            if not self.last_previewed_item:
                return

            if not urls:
                QMessageBox.warning(self, "警告", "未能获取到有效的缩略图地址。")
                return

            thumbnail_data = [
                {"url": url, "serial": str(i),
                 "title": self.last_previewed_item['ztitle']}
                for i, url in enumerate(urls, 1) if url
            ]

            if not thumbnail_data:
                QMessageBox.warning(self, "警告", "缩略图数据为空。")
                return

            # Use a single shot timer to ensure the new dialog is created in the main GUI thread cleanly.
            QTimer.singleShot(0, lambda: self._create_thumbnail_viewer(thumbnail_data))

        except Exception as e:
            logger.error(f"Error in on_thumbnail_urls_ready: {e}")
            if not self.is_shutting_down:
                QMessageBox.critical(self, "错误", f"预览窗口创建失败: {e}")

    def _create_thumbnail_viewer(self, thumbnail_data: List[Dict]):
        if self.is_shutting_down:
            return
        self.current_thumbnail_viewer = ThumbnailViewerDialog(
            thumbnail_data, self, self.image_manager, self.api_manager
        )
        self.current_thumbnail_viewer.show()

    def on_all_results_fetched(self, items: List[Dict]):
        if self.is_shutting_down:
            return
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.close()
        self.set_status_message(f"成功获取 {len(items)} 个画册，正在加入队列...")
        self._add_to_download_queue(items)

    def on_worker_error(self, message: str):
        if self.is_shutting_down:
            return
        self.set_loading_state(False)
        if hasattr(self, 'progress_dialog') and self.progress_dialog.isVisible():
            self.progress_dialog.close()
        QMessageBox.critical(self, "错误", message)
        self.update_pagination_controls()

    def prev_page(self):
        if self.current_page > 1:
            self.fetch_page_data(self.current_page - 1)

    def next_page(self):
        if self.current_page < self.total_pages:
            self.fetch_page_data(self.current_page + 1)

    def go_to_page(self):
        try:
            page = int(self.page_input.text())
            if 1 <= page <= self.total_pages:
                self.fetch_page_data(page)
        except (ValueError, TypeError):
            pass
        finally:
            self.page_input.clear()

    def fetch_page_data(self, page: int):
        self.set_loading_state(True)
        self._clear_grid()
        self.current_page = page
        self.api_manager.fetch_page(self.search_base_url, page)

    def show_album_thumbnails(self, item_data: Dict):
        count = int(item_data.get("count", 0))
        if not count:
            return QMessageBox.information(self, "提示", "该图集图片数量为0。")
        self.last_previewed_item = item_data
        self.api_manager.fetch_thumbnail_urls(
            item_data["ztitle_href"], count
        )

    def show_original_images(self, item_data: Dict):
        count = int(item_data.get("count", 0))
        if not count:
            return QMessageBox.information(self, "提示", "该图集图片数量为0。")
        self.set_status_message("正在获取原图地址...")

        try:
            self.api_manager.thumbnail_urls_ready.disconnect(
                self.on_thumbnail_urls_ready
            )
        except (TypeError, RuntimeError):
            pass

        def on_urls_ready_for_original_viewer(urls: List[str]):
            try:
                self.api_manager.thumbnail_urls_ready.disconnect(on_urls_ready_for_original_viewer)
            except (TypeError, RuntimeError):
                pass
            self.api_manager.thumbnail_urls_ready.connect(self.on_thumbnail_urls_ready)

            if self.is_shutting_down:
                return
            if not urls:
                self.set_status_message("就绪")
                return QMessageBox.warning(self, "错误", "无法获取图片地址列表。")

            self.set_status_message("获取完毕，正在打开查看器...")
            viewer = OriginalImageViewer(urls, item_data['ztitle'], count, self, self.image_manager)
            viewer.exec()
            self.set_status_message("就绪")

        self.api_manager.thumbnail_urls_ready.connect(on_urls_ready_for_original_viewer)
        self.api_manager.fetch_thumbnail_urls(item_data["ztitle_href"], count)

    def download_selected(self):
        selected_data = [widget.item_data for widget in self.thumbnail_widgets if widget.isSelected()]
        if not selected_data:
            return QMessageBox.warning(self, "提示", "请先点击选择要下载的图集。")
        self._add_to_download_queue(selected_data)

    def download_all_on_page(self):
        if not self.current_results:
            return QMessageBox.information(self, "提示", "当前页面没有可下载的项目。")
        self._add_to_download_queue(self.current_results)

    def download_all_results(self):
        if not self.search_base_url or self.total_pages <= 1:
            return QMessageBox.warning(self, "提示", "请先执行一次多页搜索或浏览。")
        reply = QMessageBox.question(
            self, "确认", f"即将从 {self.total_pages} 个页面获取所有画册信息并下载，确定吗？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.No:
            return

        self.progress_dialog = QProgressDialog(
            f"正在从 {self.total_pages} 个页面获取信息...", "取消", 0,
            self.total_pages, self
        )
        self.progress_dialog.setWindowTitle("获取全部结果")
        self.progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
        self.api_manager.all_pages_progress.connect(self.progress_dialog.setValue)
        self.progress_dialog.canceled.connect(self.api_manager.cancel_all)
        self.api_manager.fetch_all_pages(self.search_base_url, self.total_pages)
        self.progress_dialog.exec()

    def _add_to_download_queue(self, items_data: List[Dict]):
        added_count = 0
        for item_data in items_data:
            url = item_data.get("ztitle_href")
            count = int(item_data.get("count", 0))
            if not url or url in self.task_map or count == 0:
                continue

            task_item = QTreeWidgetItem([item_data["ztitle"], "排队中", "", item_data["author"]])
            task_item.setData(0, Qt.ItemDataRole.UserRole, {"url": url, "count": count})
            progress_bar = QProgressBar()
            progress_bar.setValue(0)
            progress_bar.setTextVisible(False)
            self.download_tree.addTopLevelItem(task_item)
            self.download_tree.setItemWidget(task_item, 2, progress_bar)
            self._add_item_actions(task_item)
            self.download_task_queue.append(task_item)
            self.task_map[url] = task_item
            added_count += 1

        if added_count > 0:
            self.tabs.setCurrentWidget(self.downloads_widget)
            self.set_status_message(f"已添加 {added_count} 个新任务")
            self.process_download_queue()
        else:
            self.set_status_message("所有选中项已在队列中或图片数量为0")

    def process_download_queue(self):
        self.update_downloads_tab_title()
        while (len(self.active_download_workers) < MAX_CONCURRENT_DOWNLOADS
               and self.download_task_queue):
            task_item = self.download_task_queue.popleft()
            if task_item.text(1) == "已暂停":
                self.download_task_queue.append(task_item)
                continue

            task_item.setText(1, "下载中")
            worker = DownloadWorker(task_item, self.download_directory)
            worker.progress.connect(self.on_download_progress)
            worker.finished.connect(self.on_download_finished)
            self.active_download_workers[task_item] = worker
            worker.start()

    def on_download_progress(self, task_item: QTreeWidgetItem, percentage: int):
        if pb := self.download_tree.itemWidget(task_item, 2):
            pb.setValue(percentage)

    def on_download_finished(self, task_item: QTreeWidgetItem, message: str,
                             is_success: bool):
        worker = self.active_download_workers.pop(task_item, None)

        if worker is None or self.is_shutting_down:
            return

        task_item.setText(1, message)
        task_item.setForeground(1, QColor("black") if is_success else QColor("red"))

        if cancel_btn := self._get_item_action_widget(task_item, "cancel_btn"):
            cancel_btn.setEnabled(False)
        if is_success:
            if open_dir_btn := self._get_item_action_widget(task_item, "open_dir_btn"):
                open_dir_btn.show()

        if pb := self.download_tree.itemWidget(task_item, 2):
            pb.setValue(100 if is_success else 0)

        worker.deleteLater()
        self.process_download_queue()

    def pause_all_downloads(self):
        for worker in self.active_download_workers.values():
            worker.pause()
        for item in self.download_task_queue:
            if item.text(1) == "排队中":
                item.setText(1, "已暂停")

    def resume_all_downloads(self):
        for worker in self.active_download_workers.values():
            worker.resume()
        for item in self.download_task_queue:
            if item.text(1) == "已暂停":
                item.setText(1, "排队中")
        self.process_download_queue()

    def cancel_all_downloads(self):
        for worker in list(self.active_download_workers.values()):
            worker.cancel()
        for item in self.download_task_queue:
            item.setText(1, "已取消")
        self.download_task_queue.clear()
        self.update_downloads_tab_title()

    def clear_finished_downloads(self):
        root = self.download_tree.invisibleRootItem()
        for i in range(root.childCount() - 1, -1, -1):
            item = root.child(i)
            status = item.text(1)
            is_active = item in self.active_download_workers
            is_queued = status in ("排队中", "已暂停")
            if not is_active and not is_queued:
                if url_data := item.data(0, Qt.ItemDataRole.UserRole):
                    if url := url_data.get("url"):
                        if url in self.task_map:
                            del self.task_map[url]
                root.takeChild(i)

    def update_downloads_tab_title(self):
        total = len(self.download_task_queue) + len(self.active_download_workers)
        self.tabs.setTabText(1, f"📥 下载管理 ({total})")

    def shutdown_all_workers(self):
        self.is_shutting_down = True
        self.set_status_message("正在关闭，请稍候...", timeout=0)

        all_threads = []
        all_threads.extend(self.api_manager.shutdown())
        all_threads.extend(self.image_manager.shutdown())
        for worker in list(self.active_download_workers.values()):
            worker.cancel()
            all_threads.append(worker)

        logger.info(f"Waiting for {len(all_threads)} threads to terminate...")
        for thread in all_threads:
            if thread and thread.isRunning():
                thread.wait(3000)
        logger.info("All threads have been terminated.")
        self.set_status_message("关闭完成")

    def closeEvent(self, event):
        if self.is_shutting_down:
            event.ignore()
            return

        active_tasks = len(self.active_download_workers) + len(self.download_task_queue)
        if active_tasks > 0:
            reply = QMessageBox.question(
                self, '退出确认',
                f"有 {active_tasks} 个任务仍在进行中，确定要退出吗？\n"
                "（程序将尝试取消任务并等待线程结束）",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.No:
                event.ignore()
                return

        self.shutdown_all_workers()
        event.accept()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = GalleryCrawler()
    window.show()
    sys.exit(app.exec())
