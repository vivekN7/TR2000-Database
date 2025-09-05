

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."TR2000_LOADER" AS

  PROCEDURE fetch_to_raw(
    p_url            IN VARCHAR2,
    p_cred_static_id IN VARCHAR2 DEFAULT NULL,
    p_headers        IN SYS.ODCIVARCHAR2LIST DEFAULT NULL
  ) IS
    l_json         CLOB;
    l_status       PLS_INTEGER;
    l_base_domain  VARCHAR2(255);
  BEGIN
    -- Call API via API_SERVICE gateway
    l_json := API_SERVICE.api_gateway.get_clob(
               p_url                  => p_url,
               p_method               => 'GET',
               p_body                 => NULL,
               p_headers              => p_headers,
               p_credential_static_id => p_cred_static_id,
               p_status_code          => l_status
             );

    -- Compute base domain in PL/SQL (Oracle-regex friendly)
    l_base_domain := LOWER(REGEXP_SUBSTR(p_url, '^(https?://)?([^/:]+)', 1, 1, NULL, 2));
    IF l_base_domain IS NULL THEN
      l_base_domain := 'unknown';
    END IF;

    -- Dump raw payload (no parsing)
    INSERT INTO RAW_JSON (endpoint_url, base_domain, status_code, payload)
    VALUES (p_url, l_base_domain, l_status, l_json);

    COMMIT;
  END fetch_to_raw;

END tr2000_loader;
/

