#!/usr/bin/env python3
"""根据盘点价格主数据和门店库存数据计算库存资产价值。"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

# 字段别名：支持不同文件里的列名差异
PRICE_COLUMN_ALIASES = {
    "product": ["产品名称", "产品(主键)", "产品", "商品名称", "品名"],
    "spec": ["规格"],
    "small_unit": ["小单位"],
    "small_price": ["小单位价格", "小单位单价", "单价(小单位)", "小单位价"],
    "large_unit": ["大单位"],
    "large_price": ["大单位价格", "大单位单价", "单价(大单位)", "大单位价"],
}

INVENTORY_COLUMN_ALIASES = {
    "store": ["门店名称", "门店", "门店名", "店铺", "店名"],
    "product": ["产品名称", "产品", "商品名称", "品名", "产品(主键)"],
    "qty": ["剩余数量", "库存数量", "数量", "结余数量", "剩余库存"],
    "unit": ["库存单位", "单位", "计量单位"],
}

# 全局单位兜底映射：当库存单位无法与主数据的小/大单位直接相等时使用
SMALL_UNIT_ALIASES = {"包", "斤", "袋", "盒", "瓶", "支", "个", "根", "桶"}
LARGE_UNIT_ALIASES = {"箱", "件", "提", "筐", "套", "打", "托"}


def normalize_text(value: object) -> str:
    """统一文本，去除空白和全角空格。"""
    if pd.isna(value):
        return ""
    return str(value).replace("\u3000", " ").strip()


def parse_quantity(value: object) -> float | None:
    """解析数量，支持“大半箱/小半箱=0.5箱”等文本数量。"""
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value) if pd.notna(value) else None

    text = normalize_text(value).replace(",", "")
    if not text:
        return 0.0

    compact = re.sub(r"\s+", "", text)
    if "大半箱" in compact or "小半箱" in compact:
        return 0.5

    # 支持 "0.5箱" / "1件" / "2包" 这类写法
    m = re.fullmatch(r"([+-]?\d+(?:\.\d+)?)\s*(箱|件|包|斤|个|对|袋|盒|桶|根)?", compact)
    if m:
        return float(m.group(1))

    parsed = pd.to_numeric(compact, errors="coerce")
    return float(parsed) if pd.notna(parsed) else None


def find_column(df: pd.DataFrame, candidates: Iterable[str], required: bool = True) -> str | None:
    """在 DataFrame 中按候选列表查找列名。"""
    for name in candidates:
        if name in df.columns:
            return name
    if required:
        raise ValueError(f"未找到必需字段，候选字段为: {list(candidates)}")
    return None


def read_table(path: Path, csv_encoding: str = "utf-8-sig") -> pd.DataFrame:
    """读取 CSV/Excel 文件。CSV 支持编码回退。"""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        encodings = [csv_encoding, "utf-8", "gbk", "gb18030"]
        last_error: Exception | None = None
        for enc in encodings:
            try:
                return pd.read_csv(path, encoding=enc)
            except UnicodeDecodeError as err:
                last_error = err
        raise ValueError(f"CSV 编码读取失败: {path}") from last_error
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"不支持的文件类型: {path}")


def prepare_price_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """标准化盘点价格主数据字段。"""
    df = raw_df.copy()
    df.columns = [normalize_text(col) for col in df.columns]

    product_col = find_column(df, PRICE_COLUMN_ALIASES["product"])
    spec_col = find_column(df, PRICE_COLUMN_ALIASES["spec"], required=False)
    small_unit_col = find_column(df, PRICE_COLUMN_ALIASES["small_unit"], required=False)
    small_price_col = find_column(df, PRICE_COLUMN_ALIASES["small_price"])
    large_unit_col = find_column(df, PRICE_COLUMN_ALIASES["large_unit"], required=False)
    large_price_col = find_column(df, PRICE_COLUMN_ALIASES["large_price"])

    rename_map = {
        product_col: "产品名称",
        small_price_col: "小单位价格",
        large_price_col: "大单位价格",
    }
    if spec_col:
        rename_map[spec_col] = "规格"
    if small_unit_col:
        rename_map[small_unit_col] = "小单位"
    if large_unit_col:
        rename_map[large_unit_col] = "大单位"

    df = df.rename(columns=rename_map)

    for col in ["产品名称", "规格", "小单位", "大单位"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].map(normalize_text)

    for col in ["小单位价格", "大单位价格"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["产品名称_清洗"] = df["产品名称"].map(normalize_text)

    # 主键去重：同名产品取第一条并输出告警
    duplicated_mask = df["产品名称_清洗"].duplicated(keep="first")
    if duplicated_mask.any():
        duplicated_products = sorted(df.loc[duplicated_mask, "产品名称"].unique().tolist())
        print("[警告] 盘点表存在重复产品主键，已按首条记录参与计算:")
        for name in duplicated_products:
            print(f"  - {name}")
        df = df.loc[~duplicated_mask].copy()

    return df[["产品名称_清洗", "产品名称", "规格", "小单位", "小单位价格", "大单位", "大单位价格"]]


def prepare_inventory_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """标准化门店库存字段。"""
    df = raw_df.copy()
    df.columns = [normalize_text(col) for col in df.columns]

    store_col = find_column(df, INVENTORY_COLUMN_ALIASES["store"])
    product_col = find_column(df, INVENTORY_COLUMN_ALIASES["product"])
    qty_col = find_column(df, INVENTORY_COLUMN_ALIASES["qty"])
    unit_col = find_column(df, INVENTORY_COLUMN_ALIASES["unit"])

    df = df.rename(
        columns={
            store_col: "门店",
            product_col: "产品名称",
            qty_col: "剩余数量",
            unit_col: "单位",
        }
    )

    df["门店"] = df["门店"].map(normalize_text)
    df["产品名称"] = df["产品名称"].map(normalize_text)
    df["单位"] = df["单位"].map(normalize_text)
    df["原始数量"] = df["剩余数量"].map(normalize_text)
    df["剩余数量"] = df["剩余数量"].map(parse_quantity)
    df["产品名称_清洗"] = df["产品名称"].map(normalize_text)
    df["来源行号"] = df.index + 2

    return df[["门店", "产品名称", "产品名称_清洗", "原始数量", "剩余数量", "单位", "来源行号"]]


def choose_unit_price(row: pd.Series) -> tuple[float | None, str]:
    """按库存单位判断使用小单位价格还是大单位价格。"""
    unit = normalize_text(row.get("单位", ""))
    small_unit = normalize_text(row.get("小单位", ""))
    large_unit = normalize_text(row.get("大单位", ""))

    # 1) 优先与主数据中配置的小/大单位精确匹配
    if unit and small_unit and unit == small_unit:
        return row.get("小单位价格"), "小单位(主数据匹配)"
    if unit and large_unit and unit == large_unit:
        return row.get("大单位价格"), "大单位(主数据匹配)"

    # 2) 使用通用别名兜底
    if unit in SMALL_UNIT_ALIASES:
        return row.get("小单位价格"), "小单位(通用别名)"
    if unit in LARGE_UNIT_ALIASES:
        return row.get("大单位价格"), "大单位(通用别名)"

    return None, "无法识别单位"


def build_result(price_df: pd.DataFrame, inventory_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """执行匹配、计价、汇总，返回明细和汇总表。"""
    merged = inventory_df.merge(
        price_df,
        on="产品名称_清洗",
        how="left",
        suffixes=("", "_盘点"),
    )

    unmatched_mask = merged["产品名称_盘点"].isna()
    if unmatched_mask.any():
        unmatched_products = sorted(merged.loc[unmatched_mask, "产品名称"].dropna().unique().tolist())
        print("[警告] 以下产品在盘点表中未匹配到，将无法计算金额:")
        for name in unmatched_products:
            print(f"  - {name}")

    unit_price_and_type = merged.apply(choose_unit_price, axis=1, result_type="expand")
    merged[["计算所用单价", "计价依据"]] = unit_price_and_type
    merged["计算所用单价"] = pd.to_numeric(merged["计算所用单价"], errors="coerce")

    invalid_qty_mask = merged["剩余数量"].isna()
    if invalid_qty_mask.any():
        invalid_rows = merged.loc[invalid_qty_mask, ["门店", "产品名称", "原始数量"]]
        print("[警告] 以下记录剩余数量不是有效数字，将无法计算金额:")
        for _, row in invalid_rows.iterrows():
            print(f"  - 门店={row['门店']}, 产品={row['产品名称']}, 原始数量={row['原始数量']}")

    # 仅对价格和数量都有效的记录计算金额
    merged["该项库存总价"] = merged["剩余数量"] * merged["计算所用单价"]

    unit_unrecognized_mask = (merged["计价依据"] == "无法识别单位") & (~unmatched_mask)
    if unit_unrecognized_mask.any():
        unknown_units = (
            merged.loc[unit_unrecognized_mask, ["产品名称", "单位"]]
            .drop_duplicates()
            .sort_values(["产品名称", "单位"])
        )
        print("[警告] 以下产品的库存单位无法判定为小/大单位，金额为空:")
        for _, row in unknown_units.iterrows():
            print(f"  - 产品={row['产品名称']}, 单位={row['单位']}")

    # 匹配到了产品且单位也识别成功，但主数据里价格缺失
    missing_price_mask = (
        merged["计算所用单价"].isna()
        & (~unmatched_mask)
        & (~unit_unrecognized_mask)
    )
    if missing_price_mask.any():
        missing_prices = (
            merged.loc[missing_price_mask, ["产品名称", "单位", "小单位价格", "大单位价格"]]
            .drop_duplicates()
            .sort_values(["产品名称", "单位"])
        )
        print("[警告] 以下产品已匹配到主数据，但缺少可用单价，金额为空:")
        for _, row in missing_prices.iterrows():
            print(
                f"  - 产品={row['产品名称']}, 单位={row['单位']}, "
                f"小单位价格={row['小单位价格']}, 大单位价格={row['大单位价格']}"
            )

    # 保留每一条输入记录，绝不聚合折叠
    detail_df = merged[
        ["来源行号", "门店", "产品名称", "原始数量", "剩余数量", "单位", "计算所用单价", "该项库存总价"]
    ].copy().sort_values(["来源行号", "门店", "产品名称"], na_position="last")

    store_summary_df = (
        detail_df.groupby("门店", dropna=False, as_index=False)["该项库存总价"].sum(min_count=1).rename(columns={"该项库存总价": "门店库存总资产"})
    )
    total_summary_df = pd.DataFrame(
        [{"总体库存总资产": detail_df["该项库存总价"].sum(min_count=1)}]
    )

    issue_df = merged.loc[
        unmatched_mask | invalid_qty_mask | unit_unrecognized_mask | missing_price_mask,
        [
            "来源行号",
            "门店",
            "产品名称",
            "原始数量",
            "剩余数量",
            "单位",
            "小单位",
            "大单位",
            "小单位价格",
            "大单位价格",
            "计算所用单价",
            "该项库存总价",
            "计价依据",
        ],
    ].copy()

    return detail_df, store_summary_df, total_summary_df, issue_df


def export_result(
    detail_df: pd.DataFrame,
    store_summary_df: pd.DataFrame,
    total_summary_df: pd.DataFrame,
    issue_df: pd.DataFrame,
    output_path: Path,
) -> None:
    """导出结果到 CSV 或 Excel。"""
    output_path = output_path.resolve()
    suffix = output_path.suffix.lower()

    if suffix == ".csv":
        base = output_path.with_suffix("")
        detail_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        store_summary_df.to_csv(f"{base}_门店汇总.csv", index=False, encoding="utf-8-sig")
        total_summary_df.to_csv(f"{base}_总体汇总.csv", index=False, encoding="utf-8-sig")
        if not issue_df.empty:
            issue_df.to_csv(f"{base}_异常记录.csv", index=False, encoding="utf-8-sig")

        print(f"[输出] 明细文件: {output_path}")
        print(f"[输出] 门店汇总: {base}_门店汇总.csv")
        print(f"[输出] 总体汇总: {base}_总体汇总.csv")
        if not issue_df.empty:
            print(f"[输出] 异常记录: {base}_异常记录.csv")
        return

    if suffix in {".xlsx", ".xls"}:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            detail_df.to_excel(writer, index=False, sheet_name="明细")
            store_summary_df.to_excel(writer, index=False, sheet_name="门店汇总")
            total_summary_df.to_excel(writer, index=False, sheet_name="总体汇总")
            if not issue_df.empty:
                issue_df.to_excel(writer, index=False, sheet_name="异常记录")

        print(f"[输出] Excel 文件: {output_path}")
        return

    raise ValueError("输出文件必须是 .csv/.xlsx/.xls")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="门店库存资产估值脚本")
    parser.add_argument("--price", required=True, help="盘点价格主数据文件路径（CSV/XLSX）")
    parser.add_argument("--inventory", required=True, help="门店库存文件路径（CSV/XLSX）")
    parser.add_argument("--output", default="库存估值结果.xlsx", help="输出文件路径（CSV/XLSX）")
    parser.add_argument("--csv-encoding", default="utf-8-sig", help="读取 CSV 时优先使用的编码")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    price_path = Path(args.price)
    inventory_path = Path(args.inventory)
    output_path = Path(args.output)

    if not price_path.exists():
        raise FileNotFoundError(f"盘点价格文件不存在: {price_path}")
    if not inventory_path.exists():
        raise FileNotFoundError(f"门店库存文件不存在: {inventory_path}")

    price_raw_df = read_table(price_path, csv_encoding=args.csv_encoding)
    inventory_raw_df = read_table(inventory_path, csv_encoding=args.csv_encoding)

    price_df = prepare_price_df(price_raw_df)
    inventory_df = prepare_inventory_df(inventory_raw_df)

    detail_df, store_summary_df, total_summary_df, issue_df = build_result(price_df, inventory_df)
    export_result(detail_df, store_summary_df, total_summary_df, issue_df, output_path)

    print(f"[核对] 输入库存行数: {len(inventory_df)}")
    print(f"[核对] 输出明细行数: {len(detail_df)}")

    total_amount = total_summary_df.loc[0, "总体库存总资产"]
    print(f"[完成] 总体库存总资产: {total_amount:,.2f}" if pd.notna(total_amount) else "[完成] 总体库存总资产: NaN")


if __name__ == "__main__":
    main()
