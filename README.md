# pgllm-integration
此为基于GPT服务的pg插件，通过llm，可在pg内部通过调用函数的方式提供优化建议，如sql重写，索引创建建议，分析执行计划等。
内置五个函数：
optimize_query()：规则驱动 SQL 重写
explain_query()：中文解释 SQL 语义
suggest_query_optimization()：给出优化建议
recommend_index()：给出索引建议
analyze_plan()：分析执行计划


安装与使用：设置好path路径到数据库bin目录下，
export OPENAI_API_KEY=你的key
export OPENAI_MODEL=gpt-4o(可更换为其他模型)
python3 python/llm_service.py
make/make install

CREATE EXTENSION llm_optimizer;
SET llm_optimizer.api_url = 'http://127.0.0.1:5000/analyze';

示例：
SELECT optimize_query(
'SELECT * FROM orders WHERE customer_id IN (1001) AND amount BETWEEN 100 AND 100 ORDER BY order_date DESC, order_date DESC'
);

SELECT explain_query(
'SELECT o.order_id, c.customer_name
 FROM orders o
 JOIN customers c ON o.customer_id = c.customer_id
 WHERE o.status = ''PAID''
 ORDER BY o.order_date DESC'
);

SELECT suggest_query_optimization(
'SELECT o.order_id, c.customer_name
 FROM orders o
 JOIN customers c ON o.customer_id = c.customer_id
 WHERE o.status = ''PAID''
 ORDER BY o.order_date DESC'
);

SELECT recommend_index(
'SELECT * FROM orders WHERE customer_id = 1001 AND order_date >= DATE ''2024-01-01'''
);

SELECT analyze_plan($$
Seq Scan on orders  (cost=0.00..1200.00 rows=500 width=64) (actual time=0.050..180.000 rows=500 loops=1)
  Filter: ((customer_id = 1001) AND (status = 'PAID'))
  Rows Removed by Filter: 19500
Sort  (cost=1500.00..1700.00 rows=60000 width=64) (actual time=300.000..420.000 rows=60000 loops=1)
  Sort Method: external merge  Disk: 20480kB
$$);

