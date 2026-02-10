/* Databricks SQL for SAP Material Master Transformation 
  Target: Create a unified Material Catalog view
*/

WITH 
-- 1. Base Tables with Soft Delete Filtering
mara_active AS (
    SELECT * FROM {{ source('sap_erp', 'mara') }}
    WHERE (_fivetran_deleted IS NULL OR _fivetran_deleted = false)
),

makt_active AS (
    SELECT * FROM {{ source('sap_erp', 'makt') }} 
    WHERE (_fivetran_deleted IS NULL OR _fivetran_deleted = false)
),

marc_active AS (
    SELECT * FROM {{ source('sap_erp', 'marc') }} 
    WHERE (_fivetran_deleted IS NULL OR _fivetran_deleted = false)
),

mard_active AS (
    SELECT * FROM {{ source('sap_erp', 'mard') }} 
    WHERE (_fivetran_deleted IS NULL OR _fivetran_deleted = false)
),

-- 2. Material Description Logic (Language Fallback)
-- 日本語(J)を優先し、なければ英語(E)を採用するロジック
-- ROW_NUMBER()を使って、品目ごとに優先度1位の行だけを取得します
makt_prioritized AS (
    SELECT 
        mandt,
        matnr,
        maktx,
        spras,
        ROW_NUMBER() OVER (
            PARTITION BY mandt, matnr 
            ORDER BY CASE 
                WHEN spras = 'J' THEN 1  -- 日本語優先
                WHEN spras = 'E' THEN 2  -- 次に英語
                ELSE 3                   -- その他
            END
        ) as rn
    FROM makt_active
    WHERE spras IN ('J', 'E') -- 必要に応じて対象言語を調整
)

-- 3. Main Transformation Query
-- 品目 × プラント × 保管場所 の粒度で結合
SELECT 
    -- IDs (SAP Keys)
    mara.mandt AS client_id,
    mara.matnr AS material_number,
    marc.werks AS plant_id,
    mard.lgort AS storage_location_id,

    -- Attributes (Renamed for readability)
    desc.maktx AS material_description,     -- 品目名
    mara.mtart AS material_type,            -- 品目タイプ (例: FERT, ROH)
    mara.matkl AS material_group,           -- 品目グループ
    mara.meins AS base_unit_of_measure,     -- 基本数量単位
    
    -- Status & Auditing
    mara.ersda AS created_date,
    mara.ernam AS created_by,
    mara.laeda AS last_changed_date

FROM mara_active AS mara

-- テキスト情報の結合 (1対1になるようにフィルタ済み)
LEFT JOIN makt_prioritized AS desc
    ON mara.mandt = desc.mandt 
    AND mara.matnr = desc.matnr
    AND desc.rn = 1

-- プラント情報の結合 (1対多)
-- プラント情報がない品目も含める場合はLEFT JOIN
LEFT JOIN marc_active AS marc
    ON mara.mandt = marc.mandt 
    AND mara.matnr = marc.matnr

-- 保管場所情報の結合 (1対多)
-- 保管場所定義がない場合も含めるためLEFT JOIN
LEFT JOIN mard_active AS mard
    ON marc.mandt = mard.mandt 
    AND marc.matnr = mard.matnr
    AND marc.werks = mard.werks

-- クライアントフィルタ（通常は本番クライアント指定などを行う）
-- WHERE mara.mandt = '100' 