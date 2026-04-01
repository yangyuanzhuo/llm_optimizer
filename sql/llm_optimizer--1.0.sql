CREATE FUNCTION llm_optimize_query(text)
RETURNS text
AS 'MODULE_PATHNAME', 'llm_optimize_query'
LANGUAGE C
STRICT
VOLATILE
PARALLEL UNSAFE;

CREATE FUNCTION llm_explain_query(text)
RETURNS text
AS 'MODULE_PATHNAME', 'llm_explain_query'
LANGUAGE C
STRICT
VOLATILE
PARALLEL UNSAFE;

CREATE FUNCTION llm_suggest_optimization(text)
RETURNS text
AS 'MODULE_PATHNAME', 'llm_suggest_optimization'
LANGUAGE C
STRICT
VOLATILE
PARALLEL UNSAFE;

CREATE FUNCTION llm_recommend_index(text)
RETURNS text
AS 'MODULE_PATHNAME', 'llm_recommend_index'
LANGUAGE C
STRICT
VOLATILE
PARALLEL UNSAFE;

CREATE FUNCTION llm_analyze_plan(text)
RETURNS text
AS 'MODULE_PATHNAME', 'llm_analyze_plan'
LANGUAGE C
STRICT
VOLATILE
PARALLEL UNSAFE;

CREATE FUNCTION optimize_query(sql text)
RETURNS text
LANGUAGE SQL
STRICT
VOLATILE
PARALLEL UNSAFE
AS $$
    SELECT llm_optimize_query($1);
$$;

CREATE FUNCTION explain_query(sql text)
RETURNS json
LANGUAGE SQL
STRICT
VOLATILE
PARALLEL UNSAFE
AS $$
    SELECT json_build_object(
        'explanation',
        COALESCE(llm_explain_query($1), '无法生成查询解释')
    );
$$;

CREATE FUNCTION suggest_query_optimization(sql text)
RETURNS json
LANGUAGE SQL
STRICT
VOLATILE
PARALLEL UNSAFE
AS $$
    SELECT json_build_object(
        'suggestions',
        COALESCE(llm_suggest_optimization($1), '无法生成优化建议')
    );
$$;

CREATE FUNCTION recommend_index(sql text)
RETURNS json
LANGUAGE SQL
STRICT
VOLATILE
PARALLEL UNSAFE
AS $$
    SELECT llm_recommend_index($1)::json;
$$;

CREATE FUNCTION analyze_plan(plan text)
RETURNS json
LANGUAGE SQL
STRICT
VOLATILE
PARALLEL UNSAFE
AS $$
    SELECT json_build_object(
        'analysis',
        COALESCE(llm_analyze_plan($1), 'analysis failed')
    );
$$;

COMMENT ON FUNCTION llm_optimize_query(text)
IS 'Internal C entry point for rule-based SQL rewrite.';

COMMENT ON FUNCTION llm_explain_query(text)
IS 'Internal C entry point for LLM-assisted query explanation.';

COMMENT ON FUNCTION llm_suggest_optimization(text)
IS 'Internal C entry point for LLM-assisted query optimization suggestions.';

COMMENT ON FUNCTION llm_recommend_index(text)
IS 'Internal C entry point for LLM index recommendation.';

COMMENT ON FUNCTION llm_analyze_plan(text)
IS 'Internal C entry point for LLM plan analysis.';

COMMENT ON FUNCTION optimize_query(text)
IS 'Rewrite SQL using deterministic local rewrite rules.';

COMMENT ON FUNCTION explain_query(text)
IS 'Explain query intent and structure as JSON.';

COMMENT ON FUNCTION suggest_query_optimization(text)
IS 'Return LLM-assisted SQL optimization suggestions as JSON.';

COMMENT ON FUNCTION recommend_index(text)
IS 'Return index recommendations as JSON.';

COMMENT ON FUNCTION analyze_plan(text)
IS 'Return plan analysis results as JSON.';
