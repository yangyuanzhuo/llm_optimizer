
import json
import os
import re
from typing import Dict, List, Any

from flask import Flask, request, jsonify
import openai

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False
if hasattr(app, "json"):
    app.json.ensure_ascii = False

# 配置 - 可替换为其他大模型
openai.api_key = "your-api-key"  # 实际使用时配置
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "your-api-key")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL")
MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4o")
LOG_LLM_FALLBACKS = os.getenv("LLM_LOG_FALLBACKS", "0") == "1"
openai.api_key = OPENAI_API_KEY
if OPENAI_BASE_URL:
    openai.base_url = OPENAI_BASE_URL
OPENAI_CLIENT = (
    OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)
    if OpenAI is not None else None
)


def request_llm_json(prompt: str, temperature: float, max_tokens: int) -> Dict[str, Any]:
    """Use the modern OpenAI client when available and keep a legacy fallback."""
    if OPENAI_CLIENT is not None:
        response = OPENAI_CLIENT.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
            max_tokens=max_tokens
        )
        content = response.choices[0].message.content
    else:
        response = openai.ChatCompletion.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
            max_tokens=max_tokens
        )
        content = response.choices[0].message.content

    if not content:
        raise ValueError("empty response content from LLM")

    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

    return json.loads(content)


def log_llm_fallback(message: str) -> None:
    """Emit fallback logs only when explicitly enabled."""
    if LOG_LLM_FALLBACKS:
        app.logger.warning(message)


def request_llm_field(prompt: str, field_name: str, temperature: float, max_tokens: int) -> str:
    """Request a JSON payload from the LLM and extract a single text field."""
    result = request_llm_json(prompt, temperature=temperature, max_tokens=max_tokens)
    value = result.get(field_name, "")
    if isinstance(value, list):
        return "\n".join(str(item) for item in value)
    return str(value).strip()


def local_semantic_parse(sql: str) -> Dict[str, Any]:
    """Best-effort parser used when the LLM is unavailable."""
    normalized_sql = sql.strip()
    result = {
        "tables": [],
        "predicates": [],
        "joins": [],
        "group_by": [],
        "order_by": [],
        "projection": []
    }

    from_match = re.search(r"\bFROM\s+([a-zA-Z_][\w]*)", normalized_sql, flags=re.IGNORECASE)
    if from_match:
        result["tables"].append(from_match.group(1))

    select_match = re.search(r"\bSELECT\s+(.*?)\s+\bFROM\b", normalized_sql, flags=re.IGNORECASE | re.DOTALL)
    if select_match:
        projection = [item.strip() for item in select_match.group(1).split(",")]
        result["projection"] = [item for item in projection if item]

    where_match = re.search(
        r"\bWHERE\s+(.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|$)",
        normalized_sql,
        flags=re.IGNORECASE | re.DOTALL
    )
    if where_match:
        where_clause = where_match.group(1).strip()
        predicate_parts = re.split(r"\s+AND\s+", where_clause, flags=re.IGNORECASE)
        for part in predicate_parts:
            part = part.strip().strip("()")
            match = re.match(
                r"([a-zA-Z_][\w\.]*)\s*(=|>=|<=|>|<|LIKE)\s*('?[^']*'?|\d+(?:\.\d+)?)",
                part,
                flags=re.IGNORECASE
            )
            if not match:
                continue

            column = match.group(1).split(".")[-1]
            operator = match.group(2).upper()
            value = match.group(3).strip("'")
            if operator == "=":
                predicate_type = "equality"
            elif operator == "LIKE":
                predicate_type = "like"
            else:
                predicate_type = "range"

            result["predicates"].append({
                "column": column,
                "operator": operator,
                "value": value,
                "type": predicate_type,
                "frequency": 1.0
            })

    group_match = re.search(
        r"\bGROUP\s+BY\s+(.*?)(?:\bORDER\s+BY\b|\bLIMIT\b|$)",
        normalized_sql,
        flags=re.IGNORECASE | re.DOTALL
    )
    if group_match:
        result["group_by"] = [item.strip() for item in group_match.group(1).split(",") if item.strip()]

    order_match = re.search(
        r"\bORDER\s+BY\s+(.*?)(?:\bLIMIT\b|$)",
        normalized_sql,
        flags=re.IGNORECASE | re.DOTALL
    )
    if order_match:
        result["order_by"] = [item.strip() for item in order_match.group(1).split(",") if item.strip()]

    join_matches = re.finditer(
        r"\bJOIN\s+([a-zA-Z_][\w]*)\s+\bON\b\s+(.*?)(?=\bJOIN\b|\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|$)",
        normalized_sql,
        flags=re.IGNORECASE | re.DOTALL
    )
    left_table = result["tables"][0] if result["tables"] else ""
    for join_match in join_matches:
        result["joins"].append({
            "left": left_table,
            "right": join_match.group(1),
            "condition": join_match.group(2).strip()
        })

    return result


def semantic_parse(sql: str, schema: dict) -> Dict[str, Any]:
    """
    提取语义元组 M = (T, P, J, G, O, L)
    对应论文公式(4-2)语义元组定义
    
    返回格式:
    {
        "tables": ["table1", "table2"],
        "predicates": [{"column": "col1", "operator": ">", "value": "100", "type": "range"}],
        "joins": [{"left": "t1", "right": "t2", "condition": "t1.id = t2.id"}],
        "group_by": ["col1"],
        "order_by": ["col1"],
        "projection": ["col1", "col2"]
    }
    """
    prompt = f"""
分析以下SQL语句，提取结构化信息。只返回JSON格式结果。

SQL: {sql}
表结构: {json.dumps(schema, ensure_ascii=False)}

返回JSON格式：
{{
    "tables": ["表名列表"],
    "predicates": [{{"column": "列名", "operator": "操作符", "value": "值", "type": "equality/range/like"}}],
    "joins": [{{"left": "左表", "right": "右表", "condition": "连接条件"}}],
    "group_by": ["分组列"],
    "order_by": ["排序列"],
    "projection": ["投影列"]
}}
"""
    try:
        result = request_llm_json(prompt, temperature=0.1, max_tokens=2000)
        return result
    except Exception as e:
        print(f"语义解析失败: {e}")
        return {"tables": [], "predicates": [], "joins": [], 
                "group_by": [], "order_by": [], "projection": []}


def semantic_parse(sql: str, schema: dict) -> Dict[str, Any]:
    """Extract structured SQL semantics with an LLM and a local fallback."""
    prompt = f"""
Analyze the following SQL statement and extract structured information.
Return JSON only.

SQL: {sql}
Schema: {json.dumps(schema, ensure_ascii=False)}

Return this JSON shape:
{{
    "tables": ["table_name"],
    "predicates": [{{"column": "column_name", "operator": "=", "value": "1001", "type": "equality/range/like"}}],
    "joins": [{{"left": "left_table", "right": "right_table", "condition": "a.id = b.a_id"}}],
    "group_by": ["column_name"],
    "order_by": ["column_name"],
    "projection": ["column_name"]
}}
"""
    try:
        result = request_llm_json(prompt, temperature=0.1, max_tokens=2000)
        return result
    except Exception as e:
        fallback = local_semantic_parse(sql)
        log_llm_fallback(f"semantic parse fallback used: {e}. fallback={fallback}")
        return fallback


def legacy_rewrite_sql_unused(sql: str, schema: dict) -> str:
    """
    SQL重写 - 应用等价变换规则
    对应论文4.2节查询重写，基于关系代数等价变换公式(4-2)
    数学基础: ∀ D, E(D) = E'(D)
    """
    semantic = semantic_parse(sql, schema)
    rewritten = sql
    
    # 规则1: 范围条件合并
    # 数学条件: col > a AND col < b 且 a < b → col BETWEEN a AND b
    range_pattern = r"(\w+)\s*>\s*(\d+)\s+AND\s+\1\s*<\s*(\d+)"
    
    def merge_range(match):
        col = match.group(1)
        a = int(match.group(2))
        b = int(match.group(3))
        if a < b:  # 数学条件 a < b 的代码实现
            return f"{col} BETWEEN {a} AND {b}"
        return match.group(0)
    
    rewritten = re.sub(range_pattern, merge_range, rewritten, flags=re.IGNORECASE)
    
    # 规则2: OR条件转UNION (简化实现)
    # 对应关系代数: σ_{p1 ∨ p2}(R) = σ_{p1}(R) ∪ σ_{p2}(R)
    if ' OR ' in rewritten.upper() and len(semantic.get('tables', [])) == 1:
        # 提取OR条件两侧的谓词
        parts = re.split(r'\s+OR\s+', rewritten, flags=re.IGNORECASE)
        if len(parts) == 2:
            table = semantic['tables'][0]
            union_sql = f"SELECT * FROM {table} WHERE {parts[0]} UNION SELECT * FROM {table} WHERE {parts[1]}"
            # 简单判断是否等价（实际需更严格校验）
            rewritten = union_sql
    
    # 规则3: 常量折叠
    # 数学变换: 1+2 → 3
    rewritten = re.sub(r"(\d+)\s*\+\s*(\d+)", 
                       lambda m: str(int(m.group(1)) + int(m.group(2))), 
                       rewritten)
    
    return rewritten



def rewrite_sql(sql: str, schema: dict) -> str:
    """
    SQL rewrite rules focused on safe, low-risk transformations.
    """
    rewritten = sql
    literal_pattern = (
        r"(?:"
        r"DATE\s+'[^']+'|"
        r"TIMESTAMP\s+'[^']+'|"
        r"'[^']*'|"
        r"\d+(?:\.\d+)?"
        r")"
    )

    def fold_numeric_addition(text: str) -> str:
        pattern = re.compile(r"(?<![\w'])((?:\d+(?:\.\d+)?)\s*\+\s*(?:\d+(?:\.\d+)?))(?![\w'])")

        def replace(match):
            left_text, right_text = re.split(r"\s*\+\s*", match.group(1), maxsplit=1)
            left = float(left_text)
            right = float(right_text)
            result = left + right
            return str(int(result)) if result.is_integer() else str(result)

        previous = None
        current = text
        while current != previous:
            previous = current
            current = pattern.sub(replace, current)
        return current

    def parse_sortable_literal(value: str):
        numeric_match = re.fullmatch(r"\d+(?:\.\d+)?", value)
        if numeric_match:
            return float(value)

        date_match = re.fullmatch(r"(?:DATE|TIMESTAMP)\s+'([^']+)'", value, flags=re.IGNORECASE)
        if date_match:
            return date_match.group(1)

        string_match = re.fullmatch(r"'([^']*)'", value)
        if string_match:
            return string_match.group(1)

        return None

    def swap_bounds_if_needed(low: str, high: str) -> tuple[str, str]:
        low_value = parse_sortable_literal(low)
        high_value = parse_sortable_literal(high)
        if low_value is not None and high_value is not None and low_value > high_value:
            return high, low
        return low, high

    def merge_inclusive_ranges(text: str) -> str:
        patterns = [
            re.compile(
                rf"(?P<col>[A-Za-z_][\w\.]*)\s*>=\s*(?P<low>{literal_pattern})\s+AND\s+(?P=col)\s*<=\s*(?P<high>{literal_pattern})",
                flags=re.IGNORECASE
            ),
            re.compile(
                rf"(?P<col>[A-Za-z_][\w\.]*)\s*<=\s*(?P<high>{literal_pattern})\s+AND\s+(?P=col)\s*>=\s*(?P<low>{literal_pattern})",
                flags=re.IGNORECASE
            ),
        ]

        def replace(match):
            low, high = swap_bounds_if_needed(match.group('low'), match.group('high'))
            return f"{match.group('col')} BETWEEN {low} AND {high}"

        result = text
        for pattern in patterns:
            result = pattern.sub(replace, result)
        return result

    def normalize_exclusive_ranges(text: str) -> str:
        patterns = [
            re.compile(
                rf"(?P<col>[A-Za-z_][\w\.]*)\s*>\s*(?P<low>{literal_pattern})\s+AND\s+(?P=col)\s*<\s*(?P<high>{literal_pattern})",
                flags=re.IGNORECASE
            ),
            re.compile(
                rf"(?P<col>[A-Za-z_][\w\.]*)\s*<\s*(?P<high>{literal_pattern})\s+AND\s+(?P=col)\s*>\s*(?P<low>{literal_pattern})",
                flags=re.IGNORECASE
            ),
        ]

        def replace(match):
            low, high = swap_bounds_if_needed(match.group('low'), match.group('high'))
            return f"{match.group('col')} > {low} AND {match.group('col')} < {high}"

        result = text
        for pattern in patterns:
            result = pattern.sub(replace, result)
        return result

    def normalize_mixed_ranges(text: str) -> str:
        patterns = [
            re.compile(
                rf"(?P<col>[A-Za-z_][\w\.]*)\s*>=\s*(?P<low>{literal_pattern})\s+AND\s+(?P=col)\s*<\s*(?P<high>{literal_pattern})",
                flags=re.IGNORECASE
            ),
            re.compile(
                rf"(?P<col>[A-Za-z_][\w\.]*)\s*<\s*(?P<high>{literal_pattern})\s+AND\s+(?P=col)\s*>=\s*(?P<low>{literal_pattern})",
                flags=re.IGNORECASE
            ),
            re.compile(
                rf"(?P<col>[A-Za-z_][\w\.]*)\s*>\s*(?P<low>{literal_pattern})\s+AND\s+(?P=col)\s*<=\s*(?P<high>{literal_pattern})",
                flags=re.IGNORECASE
            ),
            re.compile(
                rf"(?P<col>[A-Za-z_][\w\.]*)\s*<=\s*(?P<high>{literal_pattern})\s+AND\s+(?P=col)\s*>\s*(?P<low>{literal_pattern})",
                flags=re.IGNORECASE
            ),
        ]

        def replace(match):
            matched = match.group(0)
            col = match.group("col")
            low, high = swap_bounds_if_needed(match.group("low"), match.group("high"))
            if ">=" in matched and "<" in matched and "<=" not in matched:
                return f"{col} >= {low} AND {col} < {high}"
            return f"{col} > {low} AND {col} <= {high}"

        result = text
        for pattern in patterns:
            result = pattern.sub(replace, result)
        return result

    def dedupe_adjacent_conditions(text: str) -> str:
        comparison = rf"[A-Za-z_][\w\.]*\s*(?:=|>=|<=|>|<|LIKE|ILIKE)\s*{literal_pattern}"
        duplicate_pattern = re.compile(
            rf"(?P<expr>{comparison})\s+AND\s+(?P=expr)",
            flags=re.IGNORECASE
        )

        previous = None
        current = text
        while current != previous:
            previous = current
            current = duplicate_pattern.sub(r"\g<expr>", current)
        return current

    def normalize_in_list(text: str) -> str:
        in_pattern = re.compile(
            rf"(?P<col>[A-Za-z_][\w\.]*)\s+IN\s*\((?P<items>(?:\s*{literal_pattern}\s*,)*\s*{literal_pattern}\s*)\)",
            flags=re.IGNORECASE
        )

        def replace(match):
            raw_items = [item.strip() for item in match.group("items").split(",")]
            deduped_items = []
            seen = set()
            for item in raw_items:
                key = item.lower() if item.startswith("'") or item.upper().startswith(("DATE ", "TIMESTAMP ")) else item
                if key not in seen:
                    seen.add(key)
                    deduped_items.append(item)
            return f"{match.group('col')} IN ({', '.join(deduped_items)})"

        result = text
        return in_pattern.sub(replace, result)

    def collapse_singleton_in_list(text: str) -> str:
        singleton_pattern = re.compile(
            rf"(?P<col>[A-Za-z_][\w\.]*)\s+IN\s*\(\s*(?P<item>{literal_pattern})\s*\)",
            flags=re.IGNORECASE
        )
        return singleton_pattern.sub(r"\g<col> = \g<item>", text)

    def collapse_degenerate_between(text: str) -> str:
        between_pattern = re.compile(
            rf"(?P<col>[A-Za-z_][\w\.]*)\s+BETWEEN\s+(?P<low>{literal_pattern})\s+AND\s+(?P<high>{literal_pattern})",
            flags=re.IGNORECASE
        )

        def replace(match):
            low = match.group("low").strip()
            high = match.group("high").strip()
            if low.lower() == high.lower():
                return f"{match.group('col')} = {low}"
            return match.group(0)

        return between_pattern.sub(replace, text)

    def simplify_equality_with_singleton_membership(text: str) -> str:
        patterns = [
            re.compile(
                rf"(?P<col>[A-Za-z_][\w\.]*)\s*=\s*(?P<value>{literal_pattern})\s+AND\s+(?P=col)\s+IN\s*\(\s*(?P=value)\s*\)",
                flags=re.IGNORECASE
            ),
            re.compile(
                rf"(?P<col>[A-Za-z_][\w\.]*)\s+IN\s*\(\s*(?P<value>{literal_pattern})\s*\)\s+AND\s+(?P=col)\s*=\s*(?P=value)",
                flags=re.IGNORECASE
            ),
        ]

        result = text
        for pattern in patterns:
            result = pattern.sub(r"\g<col> = \g<value>", result)
        return result

    def split_top_level_csv(items_text: str) -> List[str]:
        items = []
        current = []
        depth = 0
        in_single_quote = False
        i = 0

        while i < len(items_text):
            ch = items_text[i]
            if ch == "'" and (i == 0 or items_text[i - 1] != "\\"):
                in_single_quote = not in_single_quote
                current.append(ch)
            elif not in_single_quote and ch == "(":
                depth += 1
                current.append(ch)
            elif not in_single_quote and ch == ")":
                depth = max(depth - 1, 0)
                current.append(ch)
            elif not in_single_quote and depth == 0 and ch == ",":
                item = "".join(current).strip()
                if item:
                    items.append(item)
                current = []
            else:
                current.append(ch)
            i += 1

        tail = "".join(current).strip()
        if tail:
            items.append(tail)
        return items

    def dedupe_order_by_items(text: str) -> str:
        order_by_pattern = re.compile(
            r"\bORDER\s+BY\s+(?P<items>.*?)(?=(\bLIMIT\b|\bOFFSET\b|\bFETCH\b|\bFOR\b|$))",
            flags=re.IGNORECASE | re.DOTALL
        )

        def replace(match):
            raw_items = split_top_level_csv(match.group("items").strip())
            deduped_items = []
            seen = set()
            for item in raw_items:
                key = re.sub(r"\s+", " ", item).strip().lower()
                if key not in seen:
                    seen.add(key)
                    deduped_items.append(item.strip())
            return "ORDER BY " + ", ".join(deduped_items)

        return order_by_pattern.sub(replace, text)

    def dedupe_group_by_items(text: str) -> str:
        group_by_pattern = re.compile(
            r"\bGROUP\s+BY\s+(?P<items>.*?)(?=(\bORDER\s+BY\b|\bLIMIT\b|\bOFFSET\b|\bFETCH\b|\bHAVING\b|$))",
            flags=re.IGNORECASE | re.DOTALL
        )

        def replace(match):
            raw_items = split_top_level_csv(match.group("items").strip())
            deduped_items = []
            seen = set()
            for item in raw_items:
                key = re.sub(r"\s+", " ", item).strip().lower()
                if key not in seen:
                    seen.add(key)
                    deduped_items.append(item.strip())
            return "GROUP BY " + ", ".join(deduped_items)

        return group_by_pattern.sub(replace, text)

    def remove_trivial_true_predicates(text: str) -> str:
        patterns = [
            (re.compile(r"\bWHERE\s+(?:1\s*=\s*1|TRUE)\s+AND\s+", flags=re.IGNORECASE), "WHERE "),
            (re.compile(r"\s+AND\s+(?:1\s*=\s*1|TRUE)(?=(\s+AND|\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s+OFFSET|\s+FETCH|\)|$))", flags=re.IGNORECASE), ""),
            (re.compile(r"\bWHERE\s+(?:1\s*=\s*1|TRUE)(?=(\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s+OFFSET|\s+FETCH|$))", flags=re.IGNORECASE), ""),
        ]

        result = text
        for pattern, replacement in patterns:
            result = pattern.sub(replacement, result)

        # Clean up possible duplicated spaces introduced by predicate removal.
        result = re.sub(r"\s{2,}", " ", result)
        result = re.sub(r"\s+\)", ")", result)
        return result.strip()

    rewritten = fold_numeric_addition(rewritten)
    rewritten = merge_inclusive_ranges(rewritten)
    rewritten = normalize_mixed_ranges(rewritten)
    rewritten = normalize_exclusive_ranges(rewritten)
    rewritten = dedupe_adjacent_conditions(rewritten)
    rewritten = normalize_in_list(rewritten)
    rewritten = collapse_singleton_in_list(rewritten)
    rewritten = collapse_degenerate_between(rewritten)
    rewritten = simplify_equality_with_singleton_membership(rewritten)
    rewritten = dedupe_order_by_items(rewritten)
    rewritten = dedupe_group_by_items(rewritten)
    rewritten = remove_trivial_true_predicates(rewritten)

    return rewritten


def local_explain_query(sql: str, schema: dict) -> str:
    """Provide a deterministic explanation when the LLM is unavailable."""
    semantic = local_semantic_parse(sql)
    parts = []

    if semantic.get("tables"):
        parts.append(f"该查询主要访问表：{', '.join(semantic['tables'])}。")
    if semantic.get("joins"):
        join_targets = [join["right"] for join in semantic["joins"] if join.get("right")]
        if join_targets:
            parts.append(f"查询包含连接操作，涉及：{', '.join(join_targets)}。")
    if semantic.get("predicates"):
        predicate_desc = [f"{pred['column']} {pred['operator']} {pred['value']}" for pred in semantic["predicates"]]
        parts.append(f"主要过滤条件为：{'；'.join(predicate_desc)}。")
    if semantic.get("group_by"):
        parts.append(f"查询按 {', '.join(semantic['group_by'])} 分组。")
    if semantic.get("order_by"):
        parts.append(f"结果按 {', '.join(semantic['order_by'])} 排序。")
    if not parts:
        parts.append("该查询当前未提取出稳定的结构信息，建议结合原 SQL 人工确认查询意图。")

    return "".join(parts)


def local_suggest_query_optimization(sql: str, schema: dict) -> str:
    """Provide deterministic optimization suggestions when the LLM is unavailable."""
    semantic = local_semantic_parse(sql)
    suggestions = []

    if semantic.get("predicates"):
        equality_cols = [pred["column"] for pred in semantic["predicates"] if pred.get("operator") == "="]
        range_cols = [pred["column"] for pred in semantic["predicates"] if pred.get("operator") in {">", "<", ">=", "<="}]
        like_cols = [pred["column"] for pred in semantic["predicates"] if pred.get("operator", "").upper() == "LIKE"]

        if equality_cols:
            suggestions.append(f"优先检查等值过滤列的索引：{', '.join(dict.fromkeys(equality_cols))}。")
        if range_cols:
            suggestions.append(f"范围过滤列可以评估普通索引或复合索引：{', '.join(dict.fromkeys(range_cols))}。")
        if like_cols:
            suggestions.append(f"LIKE 条件涉及 {', '.join(dict.fromkeys(like_cols))}，如果存在前置通配符，普通 B-Tree 索引收益可能有限。")

    if semantic.get("joins"):
        join_cols = []
        for join in semantic["joins"]:
            condition = join.get("condition", "")
            join_cols.extend(extract_candidate_columns(condition))
        if join_cols:
            suggestions.append(f"检查连接列上的索引是否齐全：{', '.join(dict.fromkeys(join_cols))}。")

    if semantic.get("order_by"):
        suggestions.append(f"如果排序是高频操作，可评估 ORDER BY 列的索引：{', '.join(semantic['order_by'])}。")

    if semantic.get("group_by"):
        suggestions.append(f"如果分组数据量较大，可检查 GROUP BY 列是否适合建立索引：{', '.join(semantic['group_by'])}。")

    if not suggestions:
        suggestions.append("当前未识别出明确的结构化优化点，建议先查看 EXPLAIN ANALYZE 再结合执行计划优化。")

    return " ".join(suggestions)


def explain_query(sql: str, schema: dict) -> str:
    """Explain query intent with an LLM, falling back to local heuristics."""
    prompt = f"""
请用中文解释下面这条 SQL 的查询意图、主要过滤条件、连接关系、分组和排序逻辑。
回答要面向开发者，简洁但清晰。
只返回 JSON，格式如下：
{{"explanation": "解释内容"}}

SQL: {sql}
Schema: {json.dumps(schema, ensure_ascii=False)}
"""
    try:
        explanation = request_llm_field(prompt, "explanation", temperature=0.2, max_tokens=800)
        if explanation:
            return explanation
    except Exception as e:
        log_llm_fallback(f"query explanation fallback used: {e}")

    return local_explain_query(sql, schema)


def suggest_query_optimization(sql: str, schema: dict) -> str:
    """Ask the LLM for optimization ideas, with a local fallback."""
    prompt = f"""
请从数据库优化角度分析下面这条 SQL，并给出中文优化建议。
建议重点包括：索引、连接方式、过滤条件、排序/分组开销。
只返回 JSON，格式如下：
{{"suggestions": "优化建议内容"}}

SQL: {sql}
Schema: {json.dumps(schema, ensure_ascii=False)}
"""
    try:
        suggestions = request_llm_field(prompt, "suggestions", temperature=0.2, max_tokens=1000)
        if suggestions:
            return suggestions
    except Exception as e:
        log_llm_fallback(f"query optimization suggestion fallback used: {e}")

    return local_suggest_query_optimization(sql, schema)


def get_selectivity_for_predicate(column: str, operator: str, value: str) -> float:
    """
    获取谓词的选择率 selectivity(cond)
    对应公式(4-4)中的 selectivity(cond)
    实际应从PostgreSQL统计信息获取，这里简化实现
    """
    # 简化：根据操作符估算选择率
    if operator == '=':
        return 0.01  # 等值条件选择率低，索引收益高
    elif operator in ('>', '<', '>=', '<='):
        return 0.3   # 范围条件选择率中等
    else:
        return 0.5   # 其他条件


def compute_gain(col: str, predicates: list, stats: dict) -> float:
    """
    计算索引收益因子
    对应论文公式(4-4): Gain(col) = Σ(w_type × freq(cond) × selectivity(cond)^(-1))
    
    参数:
        col: 列名
        predicates: 谓词列表
        stats: 统计信息字典
    
    返回:
        收益因子值
    """
    # 谓词类型权重 w_type
    type_weights = {
        'equality': 1.0,   # 等值条件权重最高
        'range': 0.6,      # 范围条件权重次之
        'like': 0.3        # 模糊匹配权重较低
    }
    
    gain = 0.0
    
    for pred in predicates:
        if pred.get('column') != col:
            continue
        
   
        w_type = type_weights.get(pred.get('type', 'equality'), 0.5)
        
        freq = pred.get('frequency', 1.0)
        
        selectivity = get_selectivity_for_predicate(
            col, pred.get('operator', '='), pred.get('value', '')
        )
        

        if selectivity > 0:
            gain += w_type * freq * (1.0 / selectivity)
    
    return gain


def recommend_index(sql: str, schema: dict) -> List[Dict[str, Any]]:

    semantic = semantic_parse(sql, schema)
    if not semantic.get('predicates'):
        semantic = local_semantic_parse(sql)
    recommendations = []
    

    columns_analyzed = set()
    
    for pred in semantic.get('predicates', []):
        col_name = pred.get('column')
        if not col_name or col_name in columns_analyzed:
            continue
        
        columns_analyzed.add(col_name)
        

        gain = compute_gain(col_name, semantic['predicates'], {})
        

        if gain > 0.5:
            table = semantic['tables'][0] if semantic['tables'] else 'unknown'
            
            recommendations.append({
                'table': table,
                'column': col_name,
                'type': pred.get('type', 'equality'),
                'gain': gain,
                'sql': f"CREATE INDEX idx_{table}_{col_name} ON {table}({col_name});"
            })
    

    recommendations.sort(key=lambda x: x['gain'], reverse=True)
    
    return recommendations



def calculate_deviation(actual_time: float, estimated_cost: float) -> float:
    """
    计算代价偏差比
    对应论文公式(4-6): δ(n) = t(n) / c_total(n)
    
    参数:
        actual_time: 实际执行时间
        estimated_cost: 估算代价
    
    返回:
        偏差比值
    """
    if estimated_cost == 0:
        return 0
    # 公式(4-6) 代码实现
    return actual_time / estimated_cost


def extract_candidate_columns(expression: str) -> List[str]:
    """Extract possible column names from a plan expression."""
    if not expression:
        return []

    candidates = re.findall(
        r"([a-zA-Z_][\w\.]*)\s*(?:=|>=|<=|>|<|~~|!~~|LIKE|ILIKE)",
        expression,
        flags=re.IGNORECASE
    )
    columns = []
    for candidate in candidates:
        column = candidate.split(".")[-1]
        if column.lower() not in {"and", "or", "not", "null", "true", "false"} and column not in columns:
            columns.append(column)
    return columns


def parse_plan_nodes(plan: str) -> List[Dict[str, Any]]:
    """Parse a text EXPLAIN ANALYZE plan into node dictionaries."""
    nodes: List[Dict[str, Any]] = []
    current_node = None
    node_pattern = re.compile(
        r"(?P<node_type>[A-Za-z][A-Za-z ]+?)(?:\s+on\s+(?P<relation>[A-Za-z_][\w]*))?"
        r"\s+\(cost=(?P<startup_cost>\d+(?:\.\d+)?)\.\.(?P<total_cost>\d+(?:\.\d+)?)"
        r"\s+rows=(?P<estimated_rows>\d+)\s+width=(?P<width>\d+)\)"
        r"(?:\s+\(actual time=(?P<actual_start>\d+(?:\.\d+)?)\.\.(?P<actual_end>\d+(?:\.\d+)?)"
        r"\s+rows=(?P<actual_rows>\d+)\s+loops=(?P<loops>\d+)\))?"
    )

    for raw_line in plan.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        node_match = node_pattern.search(line)
        if node_match:
            current_node = {
                "type": node_match.group("node_type").strip(),
                "relation": node_match.group("relation"),
                "startup_cost": float(node_match.group("startup_cost")),
                "total_cost": float(node_match.group("total_cost")),
                "estimated_rows": int(node_match.group("estimated_rows")),
                "width": int(node_match.group("width")),
                "actual_start_time": float(node_match.group("actual_start")) if node_match.group("actual_start") else 0.0,
                "actual_time": float(node_match.group("actual_end")) if node_match.group("actual_end") else 0.0,
                "actual_rows": int(node_match.group("actual_rows")) if node_match.group("actual_rows") else 0,
                "loops": int(node_match.group("loops")) if node_match.group("loops") else 1,
                "filter": "",
                "index_cond": "",
                "join_filter": "",
                "hash_cond": "",
                "sort_method": "",
                "sort_space_type": "",
                "sort_space_used": 0,
                "rows_removed_by_filter": 0,
                "rows_removed_by_join_filter": 0,
                "workers_planned": 0,
                "workers_launched": 0
            }
            nodes.append(current_node)
            continue

        if current_node is None:
            continue

        if line.startswith("Filter:"):
            current_node["filter"] = line.split(":", 1)[1].strip()
        elif line.startswith("Index Cond:"):
            current_node["index_cond"] = line.split(":", 1)[1].strip()
        elif line.startswith("Join Filter:"):
            current_node["join_filter"] = line.split(":", 1)[1].strip()
        elif line.startswith("Hash Cond:"):
            current_node["hash_cond"] = line.split(":", 1)[1].strip()
        elif line.startswith("Rows Removed by Filter:"):
            match = re.search(r"(\d+)", line)
            if match:
                current_node["rows_removed_by_filter"] = int(match.group(1))
        elif line.startswith("Rows Removed by Join Filter:"):
            match = re.search(r"(\d+)", line)
            if match:
                current_node["rows_removed_by_join_filter"] = int(match.group(1))
        elif line.startswith("Workers Planned:"):
            match = re.search(r"(\d+)", line)
            if match:
                current_node["workers_planned"] = int(match.group(1))
        elif line.startswith("Workers Launched:"):
            match = re.search(r"(\d+)", line)
            if match:
                current_node["workers_launched"] = int(match.group(1))
        elif line.startswith("Sort Method:"):
            method_match = re.search(r"Sort Method:\s+(.+?)(?:\s+Memory:|\s+Disk:|$)", line)
            if method_match:
                current_node["sort_method"] = method_match.group(1).strip()
            space_match = re.search(r"(Memory|Disk):\s*(\d+)kB", line)
            if space_match:
                current_node["sort_space_type"] = space_match.group(1)
                current_node["sort_space_used"] = int(space_match.group(2))

    return nodes


def is_inefficient(node: dict) -> tuple:
    """
    低效节点判定
    对应论文公式(4-5)的三个分支条件
    
    返回: (是否低效, 原因, 修复建议)
    """
    node_type = node.get('type', '')
    
    # 条件1: SeqScan and r(n) << total rows
    # 数学条件: 顺序扫描扫描了大部分行，但实际行数远小于总行数，本应使用索引
    if node_type == 'Seq Scan':
        actual_rows = node.get('actual_rows', 0)
        total_rows = node.get('relation_rows', 1)
        # 扫描超过10%但不到90%的行（需要索引但未使用）
        if actual_rows > 0.1 * total_rows and actual_rows < 0.9 * total_rows:
            return (True, f"顺序扫描大表，扫描{actual_rows}行，占总数{actual_rows/total_rows:.1%}", 
                    f"建议在过滤列上创建索引: CREATE INDEX ON {node.get('relation')}(过滤列);")
    
    # 条件2: NestedLoop and inner rows >> threshold
    if node_type == 'Nested Loop':
        inner_rows = node.get('inner_rows', 0)
        threshold = 10000
        if inner_rows > threshold:
            return (True, f"嵌套循环内层行数过大: {inner_rows} > {threshold}", 
                    "建议改为Hash Join，或增加work_mem配置")
    
    # 条件3: 代价偏差过大 δ(n) > threshold
    actual_time = node.get('actual_time', 0)
    estimated_cost = node.get('total_cost', 1)
    deviation = calculate_deviation(actual_time, estimated_cost)
    deviation_threshold = 2.0
    
    if deviation > deviation_threshold:
        return (True, f"代价偏差过大: δ={deviation:.2f} > {deviation_threshold}", 
                "统计信息可能过期，建议执行ANALYZE")
    
    return (False, "", "")


def analyze_plan(plan: str) -> Dict[str, Any]:

    issues = []
    suggestions = []
    
    # 解析EXPLAIN输出（简化解析）
    lines = plan.split('\n')
    
    for line in lines:
        node = {}
        
        # 提取节点类型
        if 'Seq Scan' in line:
            node['type'] = 'Seq Scan'
            # 提取表名
            match = re.search(r'on (\w+)', line)
            if match:
                node['relation'] = match.group(1)
        elif 'Nested Loop' in line:
            node['type'] = 'Nested Loop'
        else:
            continue
        
        # 提取行数
        rows_match = re.search(r'rows=(\d+)', line)
        if rows_match:
            node['actual_rows'] = int(rows_match.group(1))
        
        # 提取实际时间
        time_match = re.search(r'actual time=(\d+\.?\d*)', line)
        if time_match:
            node['actual_time'] = float(time_match.group(1))
        
        # 提取估算代价
        cost_match = re.search(r'cost=\d+\.?\d*\.\.(\d+\.?\d*)', line)
        if cost_match:
            node['total_cost'] = float(cost_match.group(1))
        
        # 执行低效判定（公式(4-5)）
        is_ineff, reason, fix = is_inefficient(node)
        
        if is_ineff:
            issues.append({
                'node_type': node.get('type', 'Unknown'),
                'detail': line.strip(),
                'reason': reason,
                'fix': fix
            })
            suggestions.append(fix)
    
    # 调用大模型生成综合优化建议
    if issues:
        prompt = f"""
以下PostgreSQL执行计划存在性能问题：

{json.dumps(issues, ensure_ascii=False, indent=2)}

请给出具体的优化建议，包括：
1. 每个问题的根因分析
2. 具体的优化操作（SQL语句或配置调整）
3. 预期效果

只返回JSON格式：{{"suggestions": "优化建议内容"}}
"""
        try:
            result = request_llm_json(prompt, temperature=0.3, max_tokens=1000)
            suggestions = [result.get('suggestions', '')]
        except Exception as e:
            print(f"大模型分析失败: {e}")
    
    return {
        'issues': issues,
        'suggestions': '\n'.join(suggestions) if suggestions else '未发现明显性能问题'
    }



def is_inefficient(node: dict) -> List[tuple[str, str]]:
    """Analyze a plan node and return detected issues."""
    node_type = node.get('type', '')
    relation = node.get('relation') or 'unknown_table'
    actual_rows = max(node.get('actual_rows', 0), 0)
    estimated_rows = max(node.get('estimated_rows', 0), 0)
    loops = max(node.get('loops', 1), 1)
    total_cost = max(node.get('total_cost', 0.0), 0.0)
    actual_time = max(node.get('actual_time', 0.0), 0.0)
    rows_removed = max(node.get('rows_removed_by_filter', 0), 0)
    processed_rows = actual_rows + rows_removed
    issues: List[tuple[str, str]] = []

    if node_type == 'Seq Scan':
        filter_expr = node.get('filter', '')
        candidate_columns = extract_candidate_columns(filter_expr)
        if processed_rows >= 1000 and rows_removed > actual_rows and filter_expr:
            if candidate_columns:
                column_list = ", ".join(candidate_columns)
                fix = f"顺序扫描过滤掉了大量记录，建议优先检查 {relation}({column_list}) 上的索引。"
            else:
                fix = f"顺序扫描过滤掉了大量记录，建议检查 {relation} 的过滤条件是否缺少索引。"
            reason = f"Seq Scan 扫描 {processed_rows} 行，仅保留 {actual_rows} 行，过滤比例约 {rows_removed / processed_rows:.1%}。"
            issues.append((reason, fix))

        if processed_rows >= 10000 and rows_removed == 0 and not node.get('index_cond'):
            reason = f"Seq Scan 扫描了 {processed_rows} 行，表规模较大。"
            fix = f"如果这是高频查询，建议确认 {relation} 是否需要更合适的索引或分区策略。"
            issues.append((reason, fix))

    if node_type == 'Nested Loop':
        join_work = actual_rows * loops
        if join_work >= 50000:
            reason = f"Nested Loop 迭代次数较高，输出 {actual_rows} 行，loops={loops}，累计处理规模约 {join_work}。"
            fix = "建议检查连接列索引，必要时尝试改写 SQL 以便优化器选择 Hash Join 或 Merge Join。"
            issues.append((reason, fix))

    if node_type == 'Sort':
        sort_method = node.get('sort_method', '')
        space_type = node.get('sort_space_type', '')
        sort_space_used = node.get('sort_space_used', 0)
        if space_type == 'Disk':
            reason = f"排序使用了磁盘临时空间，Sort Method={sort_method or 'unknown'}，Disk={sort_space_used}kB。"
            fix = "建议提高 work_mem，或为 ORDER BY / GROUP BY 列建立更匹配的索引以减少外部排序。"
            issues.append((reason, fix))
        elif actual_rows >= 50000 and actual_time >= 50:
            reason = f"排序节点处理 {actual_rows} 行，耗时 {actual_time:.3f} ms。"
            fix = "建议检查排序列的索引可用性，并评估是否能提前过滤或减少排序输入。"
            issues.append((reason, fix))

    if node_type in {'Hash Join', 'Hash'} and actual_time >= 100 and actual_rows >= 50000:
        reason = f"{node_type} 节点处理数据量较大，实际耗时 {actual_time:.3f} ms。"
        fix = "建议检查连接条件两侧的数据分布和连接列索引，并确认 work_mem 是否足够。"
        issues.append((reason, fix))

    if estimated_rows > 0 and actual_rows > 0:
        row_ratio = actual_rows / estimated_rows
        if row_ratio >= 10 or row_ratio <= 0.1:
            reason = f"行数估算偏差较大，estimated_rows={estimated_rows}，actual_rows={actual_rows}，偏差比 {row_ratio:.2f}。"
            fix = f"建议对 {relation} 执行 ANALYZE，必要时提高默认统计目标以改善基数估算。"
            issues.append((reason, fix))

    deviation = calculate_deviation(actual_time, total_cost if total_cost > 0 else 1.0)
    if actual_time >= 20 and deviation > 2.0:
        reason = f"实际耗时与估算代价偏差较大，actual_time={actual_time:.3f} ms，total_cost={total_cost:.3f}，偏差比 {deviation:.2f}。"
        fix = f"建议检查 {relation} 的统计信息是否过期，并结合 EXPLAIN (ANALYZE, BUFFERS) 进一步定位瓶颈。"
        issues.append((reason, fix))

    return issues


def analyze_plan(plan: str) -> Dict[str, Any]:
    """Analyze EXPLAIN ANALYZE text and return detailed findings."""
    issues = []
    suggestions = []
    nodes = parse_plan_nodes(plan)

    for node in nodes:
        node_issues = is_inefficient(node)
        for reason, fix in node_issues:
            issue = {
                'node_type': node.get('type', 'Unknown'),
                'relation': node.get('relation'),
                'detail': (
                    f"{node.get('type', 'Unknown')} on {node.get('relation') or 'N/A'} "
                    f"(cost={node.get('startup_cost', 0):.2f}..{node.get('total_cost', 0):.2f}, "
                    f"estimated_rows={node.get('estimated_rows', 0)}, actual_rows={node.get('actual_rows', 0)}, "
                    f"loops={node.get('loops', 1)}, actual_time={node.get('actual_time', 0):.3f} ms)"
                ),
                'reason': reason,
                'fix': fix
            }
            issues.append(issue)
            if fix not in suggestions:
                suggestions.append(fix)

    if issues:
        prompt = f"""
以下 PostgreSQL 执行计划存在潜在性能问题：

{json.dumps(issues, ensure_ascii=False, indent=2)}

请给出更细致的优化建议，包括：
1. 每个问题的根因分析
2. 可以优先执行的优化动作
3. 预期收益

只返回 JSON，格式如下：
{{"suggestions": "优化建议内容"}}
"""
        try:
            result = request_llm_json(prompt, temperature=0.3, max_tokens=1000)
            llm_suggestion = result.get('suggestions', '').strip()
            if llm_suggestion:
                suggestions = [llm_suggestion]
        except Exception as e:
            log_llm_fallback(f"plan analysis LLM fallback used: {e}")

    return {
        'issues': issues,
        'suggestions': '\n'.join(suggestions) if suggestions else '未发现明显性能问题'
    }


class SlowQueryIdentifier:

    
    WEIGHT_TIME = 0.4      # w1
    WEIGHT_FREQ = 0.3      # w2
    WEIGHT_SEMANTIC = 0.3  # w3
    
    def __init__(self, time_stats=None, freq_stats=None):
        """
        初始化
        time_stats: {'min': 最小执行时间, 'max': 最大执行时间}
        freq_stats: {'min': 最小频率, 'max': 最大频率}
        """
        self.time_stats = time_stats or {'min': 0, 'max': 10000}
        self.freq_stats = freq_stats or {'min': 0, 'max': 1000}
    
    def normalize(self, x: float, min_val: float, max_val: float) -> float:
        """
        归一化函数 σ(x)
        公式(4-1): σ(x) = (x - x_min) / (x_max - x_min)
        """
        if max_val - min_val == 0:
            return 0.5
        # 公式(4-1) 中的 σ(x) 代码实现
        return (x - min_val) / (max_val - min_val)
    
    def calculate_score(self, exec_time: float, frequency: int, semantic_score: float) -> float:
        """
        计算综合评分
        公式(4-1): Score = w1·σ(t) + w2·σ(f) + w3·s
        """
        # 归一化执行时间 σ(t)
        norm_time = self.normalize(exec_time, self.time_stats['min'], self.time_stats['max'])
        
        # 归一化执行频率 σ(f)
        norm_freq = self.normalize(float(frequency), self.freq_stats['min'], self.freq_stats['max'])
        
        # 公式(4-1) 代码实现
        score = (self.WEIGHT_TIME * norm_time + 
                 self.WEIGHT_FREQ * norm_freq + 
                 self.WEIGHT_SEMANTIC * semantic_score)
        return score
    
    def is_slow_query(self, exec_time: float, frequency: int, semantic_score: float, 
                      threshold: float = 0.7) -> bool:
        """
        判断是否为慢查询
        """
        score = self.calculate_score(exec_time, frequency, semantic_score)
        return score >= threshold



@app.route('/analyze', methods=['POST'])
def analyze():
    """统一API入口"""
    data = request.get_json()
    action = data.get('action', 'rewrite')
    
    if action == 'rewrite':
        sql = data.get('sql', '')
        schema = data.get('schema', {})
        result = rewrite_sql(sql, schema)
        return jsonify({'rewritten_sql': result})

    elif action == 'explain_query':
        sql = data.get('sql', '')
        schema = data.get('schema', {})
        result = explain_query(sql, schema)
        return jsonify({'explanation': result})

    elif action == 'suggest_optimization':
        sql = data.get('sql', '')
        schema = data.get('schema', {})
        result = suggest_query_optimization(sql, schema)
        return jsonify({'suggestions': result})
    
    elif action == 'index_recommend':
        sql = data.get('sql', '')
        schema = data.get('schema', {})
        result = recommend_index(sql, schema)
        return jsonify({
            'recommendations': result,
            'recommendations_json': json.dumps(result, ensure_ascii=False)
        })
    
    elif action == 'analyze':
        plan = data.get('plan', '')
        result = analyze_plan(plan)
        return jsonify(result)
    
    return jsonify({'error': 'Unknown action'}), 400


@app.route('/health', methods=['GET'])
def health():
    """健康检查"""
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
