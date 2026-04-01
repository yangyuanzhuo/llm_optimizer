-- Deprecated manual installer.
DO $$
BEGIN
    RAISE EXCEPTION 'Use CREATE EXTENSION llm_optimizer instead of sql/00_install.sql';
END;
$$;

CREATE OR REPLACE FUNCTION optimize_query(sql text)
RETURNS text AS $$
BEGIN
    RETURN llm_optimize_query(sql);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION recommend_index(sql text)
RETURNS json AS $$
DECLARE
    result text;
BEGIN
    result := llm_recommend_index(sql);
    IF result IS NOT NULL THEN
        RETURN result::json;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analyze_plan(plan text)
RETURNS json AS $$
DECLARE
    result text;
BEGIN
    result := llm_analyze_plan(plan);
    IF result IS NOT NULL THEN
        RETURN json_build_object('analysis', result);
    END IF;
    RETURN json_build_object('analysis', '分析失败');
END;
$$ LANGUAGE plpgsql;


COMMENT ON FUNCTION optimize_query(text) IS '基于大模型优化SQL语句';
COMMENT ON FUNCTION recommend_index(text) IS '基于大模型推荐索引';
COMMENT ON FUNCTION analyze_plan(text) IS '基于大模型分析执行计划';
