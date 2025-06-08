# 导入所需的库
import os
import requests
import datetime
import fitz
import re
from dataclasses import dataclass, asdict

from utils import fin
from macro import save, updated

INDEX_CODE = '931995'
INDEX_NAME = '海通大类资产配置'

# 自定义异常类
class PDFDownloadError(Exception):
    pass

class PDFParseError(Exception):
    pass

@dataclass
class Asset:
    major_cat: str
    minor_cat: str
    code: str
    weight: str
    
@dataclass
class Portfolio:
    date: str
    assets: list[Asset]


def format_ding(portfolio: Portfolio) -> str:
    """将Portfolio数据类格式化为钉钉机器人支持的逗号分隔形式。"""
    result = []
    
    for idx, asset in enumerate(portfolio.assets, start=1):
        result.append(
            f"{idx}, {asset.major_cat}, {asset.minor_cat}, {asset.code}, {asset.weight}%"
        )
    
    return "\n".join(result)

# 定义下载PDF文件的函数
def download_pdf():
    # 定义PDF文件的URL和保存路径
    pdf_url = f'https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/zh_CN/{INDEX_CODE}factsheet.pdf'
    data_dir = 'data'
    pdf_name = f'{INDEX_CODE}factsheet'
    
    # 获取当前日期
    today = datetime.date.today().strftime("%Y-%m-%d")
    
    # 列出data目录下以931995factsheet开头的PDF文件
    if os.path.exists(data_dir):
        for file in os.listdir(data_dir):
            if file.startswith(pdf_name) and file.endswith(".pdf") and today in file:
                fin.info(f"今天的文件 {file} 已存在，不再下载。")
                return file
    
    # 生成带时间戳的文件名
    pdf_filename = f"{pdf_name}-{today}.pdf"
    pdf_path = os.path.join(data_dir, pdf_filename)

    # 检查data目录是否存在，若不存在则创建
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        fin.info(f'已创建 {data_dir} 目录')

    # 发送请求下载PDF文件
    response = requests.get(pdf_url, stream=True)
    response.raise_for_status()  # 检查请求是否成功
    
    # 将文件写入data目录
    with open(pdf_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    fin.info(f'PDF文件已成功下载到 {pdf_path}')
    return pdf_filename


def parse_pdf_text(file_name):
    if file_name is None:
        raise PDFParseError("没有有效的PDF文件可供解析。")
    data_dir = 'data'
    pdf_path = os.path.join(data_dir, file_name)
    doc = fitz.open(pdf_path)
    try:
        text = ""
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text += page.get_text()
        
        assert_percentage = text.find("资产比例")
        text = text[assert_percentage:]
        
        #fin.info("PDF文件内容已成功提取。", text)
        match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', text)
        if not match:
            raise PDFParseError("未找到符合格式的日期。")
        
        config_date = f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        fin.info(f"找到配置日期: {config_date}")
        return text, config_date
    finally:
        doc.close()


def parse_portfolio(text, config_date):
    # 创建asset和portfolio变量
    portfolio = {"date": config_date, "assets": []}
    portfolio = Portfolio(date=config_date, assets=[])

    # 提取文本行
    lines = text.split("\n")
    
    for i, line in enumerate(lines):
        if "权重" in line:
            weight_start_line_index = i
            break
    
    for i in range(weight_start_line_index + 1, weight_start_line_index + 4):
        weight_matche = re.findall(r"(\d+\.?\d*)%", lines[i])
        if not weight_matche:
            raise PDFParseError(f"在行 {i} 中未找到百分比数字。")
        portfolio.assets.append(Asset('', '', '', weight_matche[0].strip()))

    # 找到百分比数字后的行
    cate_start_index = i + 1
    
    # 填充asset_2信息
    portfolio.assets[1].minor_cat = lines[cate_start_index].strip()
    portfolio.assets[1].code = lines[cate_start_index + 1].strip()
    portfolio.assets[1].major_cat = lines[cate_start_index + 2].strip()

    # 填充asset_3信息
    portfolio.assets[2].minor_cat = lines[cate_start_index + 3].strip()
    portfolio.assets[2].code = lines[cate_start_index + 4].strip()
    portfolio.assets[2].major_cat = lines[cate_start_index + 5].strip()

    # 填充asset_1信息
    portfolio.assets[0].minor_cat = lines[cate_start_index + 6].strip()
    portfolio.assets[0].code = lines[cate_start_index + 7].strip()
    portfolio.assets[0].major_cat = lines[cate_start_index + 8].strip()

    # 输出portfolio
    fin.info(portfolio)
    return portfolio
        
def get_config(slient=False):
    try:
        file_name = download_pdf()
        fin.info(f"下载的文件名为: {file_name}")
        
        # 调用解析函数
        text, config_date = parse_pdf_text(file_name)
        fin.info(f"解析的配置日期为: {config_date}")
        
        config_updated = updated(INDEX_CODE, config_date)
        if config_updated:
            fin.ding(f"配置日期 {config_date} 已经是最新的，不需要更新。")
        else:
            fin.info(f"配置日期 {config_date} 是最新的，可以更新。")
            portfolio = parse_portfolio(text, config_date)
            save(INDEX_CODE, asdict(portfolio))
            
            title = f'{INDEX_NAME}更新到{config_date}'
            fin.info(title)
            if slient:
                return title
            else:
                fin.ding(title,format_ding(portfolio))
    except PDFDownloadError as e:
        fin.exception(e)
    except PDFParseError as e:
        fin.exception(e)
    except Exception as e:
        fin.exception(e)
        
if __name__ == "__main__":
    get_config()