import logging.handlers
import os
import logging
from logging.handlers import SMTPHandler

import traceback


class WriteLogTxt:
    _instance = None  # 儲存單例物件

    def __new__(cls, *args, **kwargs):
        """確保整個專案只會產生一個 WriteLogTxt 實例"""
        if cls._instance is None:
            cls._instance = super(WriteLogTxt, cls).__new__(cls)
            cls._instance._initialized = False  # 標記是否已初始化
        return cls._instance

    def __init__(self, file_path, file_name, mail_config=None):
        # 如果已經初始化過，就直接跳過，避免重複設定 Logger
        if self._initialized:
            return

        # 1. 處理檔名：確保有 .log 副檔名
        if not file_name.endswith(".log"):
            self.file_name = f"{file_name}.log"
        else:
            self.file_name = file_name

        self.file_path = file_path
        self.mail_config = mail_config
        self.logger = None

        self.setup_logger()
        self._initialized = True  # 標記為已初始化

    def setup_logger(self):
        # 取得目前這個檔案 (logger.py) 所在的絕對路徑，或是執行檔路徑
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # 2. 簡化路徑邏輯：集中在同一目錄以利 backupCount 清理
        if not self.file_path:
            log_folder = os.path.join(base_dir, "Logs")
        elif self.file_path.startswith("\\") or self.file_path.startswith("/"):
            log_folder = os.path.join(base_dir, self.file_path.lstrip("\\/"))
        else:
            log_folder = self.file_path

        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # 3. 設定自動滾動與清理 (30天)
        full_log_path = os.path.join(log_folder, self.file_name)
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=full_log_path,
            when="D",
            interval=1,
            backupCount=30,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(log_format)

        # 4. 取得 Logger
        self.logger = logging.getLogger("GlobalProjectLogger")
        self.logger.setLevel(logging.INFO)

        # 確保 Handler 只被添加一次
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)

            # 5. SMTPHandler (錯誤時寄信)
            if self.mail_config:
                mail_handler = SMTPHandler(
                    mailhost=self.mail_config["mail_host"],
                    fromaddr=self.mail_config["from_addr"],
                    toaddrs=self.mail_config["to_addrs"],
                    subject=self.mail_config["subject"],
                    credentials=self.mail_config["credentials"],
                    secure=(),
                )
                mail_handler.setLevel(logging.ERROR)
                mail_handler.setFormatter(log_format)
                self.logger.addHandler(mail_handler)

    def write_log_info(self, log_content):
        self.logger.info(log_content)

    def write_log_warning(self, log_content):
        self.logger.warning(log_content)

    def write_log_error(self, log_content):
        self.logger.error(log_content)

    # 在 WriteLogTxt 類別內新增此方法
    def write_log_exception(self, custom_msg=""):
        """自動抓取目前的 Exception 堆疊資訊並寫入 Log"""
        # format_exc() 會抓取最近一次發生的錯誤資訊
        error_detail = traceback.format_exc()
        full_msg = f"{custom_msg}\n詳細錯誤資訊：\n{error_detail}"
        self.logger.error(full_msg)


# --- 使用方法 ---
# 在 main.py 或專案進入點初始化一次
# log_tool = WriteLogTxt("Logs", "my_app")

# 在其他檔案 (例如 db_helper.py) 再次呼叫時，會直接拿到同一個物件
# log_tool = WriteLogTxt("Logs", "my_app")
# log_tool.write_log_info("這會記在同一個檔案裡，且不會重複初始化")


# --- 使用範例 ---
if __name__ == "__main__":
    # 郵件設定範例 (以 Gmail 為例)
    my_mail_settings = {
        "mail_host": ("smtp.gmail.com", 587),
        "from_addr": "your_service@gmail.com",
        "to_addrs": ["admin@example.com"],
        "subject": "系統錯誤通報",
        "credentials": (
            "your_account@gmail.com",
            "your_app_password",
        ),  # 需使用應用程式密碼
    }

    log_tool = WriteLogTxt(
        file_path="", file_name="test_log", mail_config=my_mail_settings
    )
    log_tool.setup_logger()

    log_tool.write_log_info("這是一條普通訊息，只會存檔。")
    log_tool.write_log_error("這是一條錯誤訊息！會存檔並寄信。")
