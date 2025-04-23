import configparser
from typing import List, Tuple, Union

import pandas as pd

__all__ = [
    "get_block_members_ths",
    "get_block_members_tdx",
    "get_signals_tdx",
]


class MyConfigParser(configparser.ConfigParser):
    def optionxform(self, optionstr):
        return optionstr

    def options_values(self, section):
        """Return a list of option names for the given section name."""
        try:
            opts = self._sections[section].copy()
        except KeyError:
            raise configparser.NoSectionError(section) from None
        opts.update(self._defaults)
        return opts


def get_ini_codes(cf: configparser.ConfigParser, block_name: str) -> List[List[str]]:
    ov = cf.options_values('BLOCK_NAME_MAP_TABLE')

    for k, v in ov.items():
        if v == block_name:
            try:
                val = cf.get("BLOCK_STOCK_CONTEXT", k)
                return [_.split(":") for _ in val.split(",") if len(_) > 0]
            except configparser.NoOptionError:
                return []

    return []


def get_block_members_ths(paths: Union[str, List[str]], block_name: str) -> List[List[str]]:
    """读取同花顺板块文件，返回市场和代码

    Parameters
    ----------
    paths: str or list of str
        同花顺板块文件。

        自定义板块 - `同花顺安装目录\mo_*\StockBlock.ini`
        系统板块 - `同花顺安装目录\system\同花顺方案\StockBlock.ini`
    block_name:str
        板块名称

    Notes
    -----
    部分板块需要两个文件结合使用，因为`板块名`和`成份股`在不同的文件中。

    例如：

    自定义板块，改名前`板块名称`在`同花顺安装目录\system\同花顺方案\StockBlock.ini`文件中
    自定义板块，改名后`板块名称`在`同花顺安装目录\mo_*\StockBlock.ini`文件中

    """
    cf = MyConfigParser()

    cf.read(paths)

    return get_ini_codes(cf, block_name)


def get_block_members_tdx(path: str) -> List[Tuple[str, str]]:
    """读取通达信板块文件，返回市场和代码

    板块文件名格式：板块简称.blk。例如：

    自选股 zxg.blk
    条件股 tjg.blk

    Parameters
    ----------
    path:str
        通达信板块文件。`通达信安装目录\T0002\blocknew\*.blk`

    """
    with open(path, "r") as f:
        return [(_[:1], _[1:]) for _ in f.read().splitlines() if len(_) > 0]


def get_signals_tdx(path: str, *, auto_export: bool) -> pd.DataFrame:
    """读取通达信预警结果，返回DataFrame

    Parameters
    ----------
    path:str
        通达信预警结果文件
    auto_export:bool
        是否预警结果自动导出的文件

    """
    if auto_export:
        kwargs = {
            "names": ["code", "name", "datetime", "price", "pct_change", "value", "condition"],
            "parse_dates": ["datetime"],
        }
    else:
        kwargs = {
            "names": ["name", "code", "time", "price", "pct_change", "condition"],
            "parse_dates": ["time"],  # 只有时间，没有日期 %H:%M
            "date_format": "mixed",  # 自动填充了当前日期
        }
    return pd.read_csv(
        path,
        sep="\t",
        header=None,
        index_col=False,
        encoding="gbk",
        dtype={'code': str},
        converters={'pct_change': lambda x: float(x.strip('%')) / 100},
        **kwargs)


if __name__ == "__main__":
    path = r"D:\海王星金融终端-中国银河证券\T0002\blocknew\zxg.blk"
    print(get_block_members_tdx(path))

    path1 = r"D:\同花顺软件\同花顺\mo_279329390\StockBlock.ini"
    path2 = r"D:\同花顺软件\同花顺\system\同花顺方案\StockBlock.ini"

    print(get_block_members_ths(path1, "昨日涨停，今日高开7%"))
    print(get_block_members_ths(path1, "今日涨停;业绩预增"))
    print(get_block_members_ths([path1, path2], "板块2"))

    path = r"D:\海王星金融终端-中国银河证券\signals.txt"
    print(get_signals_tdx(path, auto_export=True))
    path = r"d:\Users\Xxx\Desktop\signals.txt"
    print(get_signals_tdx(path, auto_export=False))
