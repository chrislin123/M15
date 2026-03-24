import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import ProjectLib as ProjectLib

# Logger
from logger import get_logger

log_obj = get_logger()

SourceBases = []
# 三地門
SourceBases.append(
    {
        "url": r"http://111.70.17.18:89/%e4%b8%89%e5%9c%b0%e9%96%80/",
        "filename": "D089",
        "foldername": "D089",
    }
)

# 六龜
SourceBases.append(
    {
        "url": r"http://111.70.17.18:89/%e5%85%ad%e9%be%9c/",
        "filename": "D047",
        "foldername": "D047",
    }
)

# 茂林01
SourceBases.append(
    {
        "url": r"http://111.70.17.18:89/%e8%8c%82%e6%9e%9701/",
        "filename": "D062",
        "foldername": "D062",
    }
)

# 茂林02
SourceBases.append(
    {
        "url": r"http://111.70.17.18:89/%e8%8c%82%e6%9e%9702/",
        "filename": "D064",
        "foldername": "D064",
    }
)


def GetLatestCCD():

    for SourceBase in SourceBases:

        # 設定目標網址
        url = SourceBase["url"]
        # 設定存放資料夾
        base_path = r"C:\FUNCTION\XML_TN\dsmon\vm"
        save_folder = os.path.join(base_path, SourceBase["foldername"].strip("/"))

        # 如果資料夾不存在則建立
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)

        try:
            # 1. 取得網頁內容
            response = requests.get(url)
            response.raise_for_status()  # 檢查請求是否成功

            # 2. 解析 HTML
            soup = BeautifulSoup(response.text, "html.parser")

            # 3. 找出所有圖片連結 (假設是 .jpg, .png 或 .gif)
            # 大多數檔案清單網頁會把檔名放在 <a> 標籤的 href 屬性中
            image_links = []
            for link in soup.find_all("a"):
                href = link.get("href")
                if href and (
                    href.endswith(".jpg")
                    or href.endswith(".png")
                    or href.endswith(".jpeg")
                ):
                    image_links.append(href)

            if not image_links:
                print("未找到任何影像檔案。")
            else:
                # 4. 取得最後一個檔案（通常網頁清單是按時間排序，最後一個為最新）
                # 如果網頁是舊到新排，取最後一個；如果是新到舊排，取第一個。
                # 這裡假設「最新」在清單最後面，你可以視網頁實際情況改用 image_links[0]
                latest_image_name = image_links[-1]
                latest_image_url = urljoin(url, latest_image_name)

                print(f"發現最新影像: {latest_image_name}")

                # 優化點 2：取得原副檔名 (例如 .jpg)，並結合自定義檔名
                ext = os.path.splitext(latest_image_name)[1]
                new_filename = f"{SourceBase['filename']}{ext}"
                file_path = os.path.join(save_folder, new_filename)

                print(f"[{save_folder}] 下載中: {latest_image_name} -> {new_filename}")

                # 5. 下載並儲存影像
                img_data = requests.get(latest_image_url, timeout=10).content

                with open(file_path, "wb") as handler:
                    handler.write(img_data)

                print(f"成功存至: {file_path}")

        except Exception as e:
            log_obj.write_log_exception(
                f"異常內容：{e}",
                f"發生異常: {type(e).__name__}",
            )
            print(f"An error occurred: {e}")


if __name__ == "__main__":

    # 下載資料同步到資料庫
    try:  # 使用Try except，預防對方主機沒有連線導致下載程序異常影響後面轉檔
        GetLatestCCD()

        log_obj.write_log_info("RunCCD,執行完成")
    except Exception as e:
        log_obj.write_log_exception(
            f"異常內容：{e}",
            f"發生異常: {type(e).__name__}",
        )
