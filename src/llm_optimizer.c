#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(llm_optimize_query);
PG_FUNCTION_INFO_V1(llm_explain_query);
PG_FUNCTION_INFO_V1(llm_suggest_optimization);
PG_FUNCTION_INFO_V1(llm_recommend_index);
PG_FUNCTION_INFO_V1(llm_analyze_plan);

static char *llm_api_url = NULL;
static double llm_confidence_threshold = 0.7;
static int llm_api_timeout_ms = 60000;

typedef struct MemoryStruct
{
    char *memory;
    size_t size;
} MemoryStruct;

static size_t
write_memory_callback(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    MemoryStruct *mem = (MemoryStruct *) userp;
    char *new_memory;

    new_memory = realloc(mem->memory, mem->size + realsize + 1);
    if (new_memory == NULL)
        return 0;

    mem->memory = new_memory;
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = '\0';

    return realsize;
}

static char *
extract_json_string_field(const char *json, const char *field)
{
    StringInfoData field_pattern;
    StringInfoData result;
    const char *pos;
    bool escaped = false;

    initStringInfo(&field_pattern);
    appendStringInfo(&field_pattern, "\"%s\"", field);

    pos = strstr(json, field_pattern.data);
    pfree(field_pattern.data);
    if (pos == NULL)
        return NULL;

    pos = strchr(pos, ':');
    if (pos == NULL)
        return NULL;
    pos++;

    while (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r')
        pos++;

    if (*pos != '"')
        return NULL;
    pos++;

    initStringInfo(&result);

    while (*pos != '\0')
    {
        if (escaped)
        {
            switch (*pos)
            {
                case '"':
                case '\\':
                case '/':
                    appendStringInfoChar(&result, *pos);
                    break;
                case 'n':
                    appendStringInfoChar(&result, '\n');
                    break;
                case 'r':
                    appendStringInfoChar(&result, '\r');
                    break;
                case 't':
                    appendStringInfoChar(&result, '\t');
                    break;
                default:
                    appendStringInfoChar(&result, *pos);
                    break;
            }
            escaped = false;
        }
        else if (*pos == '\\')
        {
            escaped = true;
        }
        else if (*pos == '"')
        {
            return result.data;
        }
        else
        {
            appendStringInfoChar(&result, *pos);
        }

        pos++;
    }

    pfree(result.data);
    return NULL;
}

static char *
call_llm_api(const char *payload)
{
    CURL *curl;
    CURLcode res;
    MemoryStruct chunk;
    struct curl_slist *headers = NULL;

    if (llm_api_url == NULL || llm_api_url[0] == '\0')
    {
        ereport(WARNING, (errmsg("llm_optimizer.api_url is not configured")));
        return NULL;
    }

    chunk.memory = malloc(1);
    if (chunk.memory == NULL)
        return NULL;
    chunk.memory[0] = '\0';
    chunk.size = 0;

    curl = curl_easy_init();
    if (curl == NULL)
    {
        free(chunk.memory);
        ereport(WARNING, (errmsg("failed to initialize libcurl")));
        return NULL;
    }

    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, llm_api_url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_memory_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &chunk);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 5000L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long) llm_api_timeout_ms);

    res = curl_easy_perform(curl);

    curl_easy_cleanup(curl);
    curl_slist_free_all(headers);

    if (res != CURLE_OK)
    {
        ereport(WARNING,
                (errmsg("LLM API call failed: %s", curl_easy_strerror(res))));
        free(chunk.memory);
        return NULL;
    }

    return chunk.memory;
}

static char *
escape_json_string(const char *str)
{
    StringInfoData buf;

    initStringInfo(&buf);

    while (*str)
    {
        if (*str == '"' || *str == '\\')
        {
            appendStringInfoChar(&buf, '\\');
            appendStringInfoChar(&buf, *str);
        }
        else if (*str == '\n')
        {
            appendStringInfoString(&buf, "\\n");
        }
        else if (*str == '\r')
        {
            appendStringInfoString(&buf, "\\r");
        }
        else if (*str == '\t')
        {
            appendStringInfoString(&buf, "\\t");
        }
        else
        {
            appendStringInfoChar(&buf, *str);
        }
        str++;
    }

    return buf.data;
}

static char *
get_schema_json(void)
{
    return pstrdup("{}");
}

Datum
llm_optimize_query(PG_FUNCTION_ARGS)
{
    text *query_text;
    char *query;
    char *response = NULL;
    char *optimized_sql = NULL;
    StringInfoData payload;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    query_text = PG_GETARG_TEXT_PP(0);
    query = text_to_cstring(query_text);

    initStringInfo(&payload);
    appendStringInfo(&payload,
                     "{\"sql\": \"%s\", \"action\": \"rewrite\"}",
                     escape_json_string(query));

    response = call_llm_api(payload.data);

    if (response != NULL)
    {
        optimized_sql = extract_json_string_field(response, "rewritten_sql");
        free(response);
    }

    pfree(payload.data);

    if (optimized_sql != NULL && strlen(optimized_sql) > 0)
        PG_RETURN_TEXT_P(cstring_to_text(optimized_sql));

    PG_RETURN_TEXT_P(query_text);
}

Datum
llm_explain_query(PG_FUNCTION_ARGS)
{
    text *query_text;
    char *query;
    char *response = NULL;
    char *explanation = NULL;
    char *schema_json;
    StringInfoData payload;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    query_text = PG_GETARG_TEXT_PP(0);
    query = text_to_cstring(query_text);
    schema_json = get_schema_json();

    initStringInfo(&payload);
    appendStringInfo(&payload,
                     "{\"sql\": \"%s\", \"action\": \"explain_query\", \"schema\": %s}",
                     escape_json_string(query),
                     schema_json);

    response = call_llm_api(payload.data);

    if (response != NULL)
    {
        explanation = extract_json_string_field(response, "explanation");
        free(response);
    }

    pfree(schema_json);
    pfree(payload.data);

    if (explanation != NULL && strlen(explanation) > 0)
        PG_RETURN_TEXT_P(cstring_to_text(explanation));

    PG_RETURN_NULL();
}

Datum
llm_suggest_optimization(PG_FUNCTION_ARGS)
{
    text *query_text;
    char *query;
    char *response = NULL;
    char *suggestions = NULL;
    char *schema_json;
    StringInfoData payload;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    query_text = PG_GETARG_TEXT_PP(0);
    query = text_to_cstring(query_text);
    schema_json = get_schema_json();

    initStringInfo(&payload);
    appendStringInfo(&payload,
                     "{\"sql\": \"%s\", \"action\": \"suggest_optimization\", \"schema\": %s}",
                     escape_json_string(query),
                     schema_json);

    response = call_llm_api(payload.data);

    if (response != NULL)
    {
        suggestions = extract_json_string_field(response, "suggestions");
        free(response);
    }

    pfree(schema_json);
    pfree(payload.data);

    if (suggestions != NULL && strlen(suggestions) > 0)
        PG_RETURN_TEXT_P(cstring_to_text(suggestions));

    PG_RETURN_NULL();
}

Datum
llm_recommend_index(PG_FUNCTION_ARGS)
{
    text *query_text;
    char *query;
    char *response = NULL;
    char *recommendation = NULL;
    char *schema_json;
    StringInfoData payload;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    query_text = PG_GETARG_TEXT_PP(0);
    query = text_to_cstring(query_text);
    schema_json = get_schema_json();

    initStringInfo(&payload);
    appendStringInfo(&payload,
                     "{\"sql\": \"%s\", \"action\": \"index_recommend\", \"schema\": %s}",
                     escape_json_string(query),
                     schema_json);

    response = call_llm_api(payload.data);

    if (response != NULL)
    {
        recommendation = extract_json_string_field(response, "recommendations_json");
        free(response);
    }

    pfree(schema_json);
    pfree(payload.data);

    if (recommendation != NULL && strlen(recommendation) > 0)
        PG_RETURN_TEXT_P(cstring_to_text(recommendation));

    PG_RETURN_NULL();
}

Datum
llm_analyze_plan(PG_FUNCTION_ARGS)
{
    text *plan_text;
    char *plan;
    char *response = NULL;
    char *analysis = NULL;
    StringInfoData payload;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    plan_text = PG_GETARG_TEXT_PP(0);
    plan = text_to_cstring(plan_text);

    initStringInfo(&payload);
    appendStringInfo(&payload,
                     "{\"plan\": \"%s\", \"action\": \"analyze\"}",
                     escape_json_string(plan));

    response = call_llm_api(payload.data);

    if (response != NULL)
    {
        analysis = extract_json_string_field(response, "suggestions");
        free(response);
    }

    pfree(payload.data);

    if (analysis != NULL)
        PG_RETURN_TEXT_P(cstring_to_text(analysis));

    PG_RETURN_NULL();
}

void
_PG_init(void)
{
    DefineCustomStringVariable("llm_optimizer.api_url",
                               "HTTP endpoint used by the llm_optimizer extension.",
                               "The endpoint must accept POST requests with JSON payloads.",
                               &llm_api_url,
                               "http://127.0.0.1:5000/analyze",
                               PGC_SUSET,
                               0,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomRealVariable("llm_optimizer.confidence_threshold",
                             "Reserved confidence threshold for planner advice.",
                             "Currently informational and kept for future ranking logic.",
                             &llm_confidence_threshold,
                             0.7,
                             0.0,
                             1.0,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomIntVariable("llm_optimizer.api_timeout_ms",
                            "HTTP timeout in milliseconds for calls to the LLM service.",
                            "Increase this if model responses regularly take longer than the default.",
                            &llm_api_timeout_ms,
                            60000,
                            1000,
                            600000,
                            PGC_SUSET,
                            0,
                            NULL,
                            NULL,
                            NULL);

    curl_global_init(CURL_GLOBAL_DEFAULT);

    ereport(LOG,
            (errmsg("llm_optimizer initialized"),
             errdetail("API URL: %s, timeout_ms: %d", llm_api_url, llm_api_timeout_ms)));
}

void
_PG_fini(void)
{
    curl_global_cleanup();
}
