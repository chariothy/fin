import akshare as ak
import pandas as pd
from utils import fin
import datetime, time
import shutil
import numpy as np

# 1. 设置参数
index_codes = ["931995", "930929", "931061", "931678"]
start_date = "20120130"
end_date = datetime.datetime.now().strftime("%Y%m%d")  # 自动获取当前日期
output_file = "指数数据.xlsx"

# 2. 获取数据并创建初始DataFrame
def fetch_index_data(symbol):
    try:
        df = ak.stock_zh_index_hist_csindex(symbol=symbol, start_date=start_date, end_date=end_date)
        df['日期'] = pd.to_datetime(df['日期'])  # 确保日期格式统一
        fin.debug(f'已获取指数{symbol}数据')
        return df[['日期', '收盘']].rename(columns={'收盘': symbol})
    except Exception as e:
        fin.ex(f"指数 {symbol} 获取失败: {str(e)}")
        return pd.DataFrame()


# 2. 定义指标计算函数
def calculate_metrics(price_series):
    """计算四项核心指标"""
    # 清理无效值
    clean_prices = price_series.dropna()
    if len(clean_prices) < 10:  # 数据不足返回空值
        return [None]*4
    
    # 计算日收益率（对数收益率更准确）
    returns = np.log(clean_prices / clean_prices.shift(1)).dropna()
    
    # 年化收益率（按250个交易日计算）
    total_return = clean_prices.iloc[-1] / clean_prices.iloc[0] - 1
    holding_days = (datetime.datetime.strptime(clean_prices.index[-1], '%Y-%m-%d') - datetime.datetime.strptime(clean_prices.index[0], '%Y-%m-%d')).days
    annual_return = (1 + total_return) ** (365 / holding_days) - 1 if holding_days > 0 else None
    
    # 年化波动率
    annual_volatility = returns.std() * np.sqrt(250)
    
    # 夏普比率（假设无风险利率2%）
    sharpe_ratio = (annual_return - 0.02) / annual_volatility if annual_volatility > 0 else None
    
    # 最大回撤
    cumulative_returns = (1 + returns).cumprod()
    peak = cumulative_returns.cummax()
    drawdown = (cumulative_returns - peak) / peak
    max_drawdown = drawdown.min()
    
    return [
        annual_return * 100,    # 年化收益率(%)
        annual_volatility * 100,# 波动率(%)
        sharpe_ratio,           # 夏普比率
        max_drawdown * 100      # 最大回撤(%)
    ]


def merge_data():
    # 3. 合并数据并日期对齐
    all_data = []
    for code in index_codes:
        df = fetch_index_data(code)
        if not df.empty:
            all_data.append(df.set_index('日期'))
        time.sleep(1)

    # 创建包含所有日期的基准索引
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    merged_df = pd.DataFrame(index=date_range)

    # 按日期对齐合并数据
    for df in all_data:
        merged_df = merged_df.join(df, how='left')

    # 4. 数据清洗和格式处理
    merged_df.reset_index(inplace=True)
    merged_df.rename(columns={'index': '日期'}, inplace=True)
    merged_df['日期'] = merged_df['日期'].dt.strftime('%Y-%m-%d')  # 统一日期格式


    # 3. 计算所有指标
    results = []
    for code in index_codes:
        if code in merged_df.columns:
            metrics = calculate_metrics(merged_df.set_index('日期')[code])
            results.append(metrics)
        else:
            results.append([None]*4)

    # 4. 创建结果数据框
    metrics_df = pd.DataFrame(
        results,
        columns=['年化收益率%', '波动率%', '夏普比率', '最大回撤%'],
        index=index_codes
    )
    metrics_df = metrics_df.T

    current_date = datetime.datetime.now().strftime("%Y%m%d")
    output_file = fin['asset_config_path']
    output_file = output_file.replace('.xlsx', f'-{current_date}.xlsx')
    try:
        shutil.copyfile(fin['asset_config_path'], output_file)
    except PermissionError:
        print("文件被占用，请关闭后重试...")
        return
    
    # 5. 导出到Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        metrics_df.to_excel(writer, sheet_name='配指', index=True, header=False)
        merged_df.to_excel(writer, sheet_name='配指', index=False, startrow=5)
        
    print(f"数据已成功导出到 {output_file}")
    
if __name__ == "__main__":
    merge_data()