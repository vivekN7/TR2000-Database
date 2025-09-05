

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "API_SERVICE"."API_GATEWAY" AS

  FUNCTION get_domain(p_url IN VARCHAR2) RETURN VARCHAR2 IS
    l_url  VARCHAR2(4000) := TRIM(p_url);
    l_host VARCHAR2(255);
  BEGIN
    -- Capture group 2 = host; group 1 = optional scheme (Oracle regex friendly)
    l_host := LOWER(
                REGEXP_SUBSTR(
                  l_url,
                  '^(https?://)?([^/:]+)',
                  1, 1, NULL, 2
                )
              );
    RETURN l_host;
  END get_domain;

  FUNCTION get_clob(
    p_url                   IN VARCHAR2,
    p_method                IN VARCHAR2,
    p_body                  IN CLOB,
    p_headers               IN SYS.ODCIVARCHAR2LIST,
    p_credential_static_id  IN VARCHAR2,
    p_status_code           OUT PLS_INTEGER
  ) RETURN CLOB
  IS
    l_resp         CLOB;
    l_base_domain  VARCHAR2(255) := NVL(get_domain(p_url), 'unknown');
    l_caller       VARCHAR2(128) := UPPER(SYS_CONTEXT('USERENV','SESSION_USER'));
    l_bytes        NUMBER;
    l_ms           NUMBER;
    l_t0           BINARY_INTEGER := DBMS_UTILITY.GET_TIME; -- centiseconds
    l_hdr_name     VARCHAR2(256);
    l_hdr_val      VARCHAR2(4000);
  BEGIN
    -- APEX context assumed; no workspace switching
    apex_web_service.g_request_headers.DELETE;
    apex_web_service.set_request_headers('Accept','application/json');

    IF p_headers IS NOT NULL THEN
      FOR i IN 1 .. p_headers.COUNT LOOP
        l_hdr_name := SUBSTR(p_headers(i), 1, INSTR(p_headers(i), ':')-1);
        l_hdr_val  := LTRIM(SUBSTR(p_headers(i), INSTR(p_headers(i), ':')+1));
        IF l_hdr_name IS NOT NULL THEN
          apex_web_service.set_request_headers(l_hdr_name, l_hdr_val);
        END IF;
      END LOOP;
    END IF;

    IF p_credential_static_id IS NOT NULL THEN
      l_resp := apex_web_service.make_rest_request(
                  p_url                  => p_url,
                  p_http_method          => p_method,
                  p_body                 => p_body,
                  p_credential_static_id => p_credential_static_id
                );
    ELSE
      l_resp := apex_web_service.make_rest_request(
                  p_url         => p_url,
                  p_http_method => p_method,
                  p_body        => p_body
                );
    END IF;

    p_status_code := apex_web_service.g_status_code;

    l_bytes := NVL(DBMS_LOB.GETLENGTH(l_resp), 0);
    l_ms    := (DBMS_UTILITY.GET_TIME - l_t0) * 10; -- ms

    MERGE INTO API_SERVICE.api_call_stats s
    USING (SELECT l_caller caller_schema, l_base_domain base_domain FROM dual) x
      ON (s.caller_schema = x.caller_schema AND s.base_domain = x.base_domain)
    WHEN MATCHED THEN UPDATE SET
      total_calls       = s.total_calls + 1,
      total_ok          = s.total_ok + CASE WHEN p_status_code BETWEEN 200 AND 299 THEN 1 ELSE 0 END,
      total_err         = s.total_err + CASE WHEN p_status_code BETWEEN 200 AND 299 THEN 0 ELSE 1 END,
      total_bytes       = s.total_bytes + l_bytes,
      last_called_at    = SYSTIMESTAMP,
      last_status       = p_status_code,
      last_duration_ms  = l_ms,
      last_url          = SUBSTR(p_url,1,1000)
    WHEN NOT MATCHED THEN INSERT (
      caller_schema, base_domain, total_calls, total_ok, total_err, total_bytes,
      last_called_at, last_status, last_duration_ms, last_url
    ) VALUES (
      l_caller, l_base_domain, 1,
      CASE WHEN p_status_code BETWEEN 200 AND 299 THEN 1 ELSE 0 END,
      CASE WHEN p_status_code BETWEEN 200 AND 299 THEN 0 ELSE 1 END,
      l_bytes, SYSTIMESTAMP, p_status_code, l_ms, SUBSTR(p_url,1,1000)
    );

    RETURN l_resp;
  END get_clob;

END api_gateway;
/

