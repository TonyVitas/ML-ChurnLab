WITH
months AS (
    SELECT LAST_DAY(ADD_MONTHS(:START_DATE, LEVEL - 1)) AS MONTH_END
    FROM   DUAL
    CONNECT BY LEVEL <= 60 -- CHANGE here NUMBER OF months
),
card_snapshot AS (
    SELECT
        c.CARD_ID,
        c.AS_OF_DT              AS MONTH_END,
        c.CUST_ID,
        c.STATUS,
        c.BRCH_CODE,
        c.CARD_LIMIT,
        c.TOTAL_EXPOSURE_AMT,
        c.OVERDUE_AMT_RON,
        c.FUTURE_INST_AMT,
        c.BONUS_AMT,
        c.DPD,
        c.TIMES_IN_5_DPD,
        c.TIMES_IN_30_DPD,
        c.TIMES_IN_60_DPD,
        c.TIMES_IN_90_DPD,
        c.TIMES_IN_90_PLUS_DPD,
        c.LAST_EXPOSURE_DATE,
        c.SENT_TO_LEGAL,
        c.DOD,
        c.POOL_RT,
        c.ILOE,
        c.DOUBTFUL_INT_AMT,
        c.DOUBTFUL_INT_PEN_AMT,
        c.DOUBTFUL_PRIN_AMT,
        c.DOUBTFUL_PRIN_PEN_AMT,
        c.FUTURE_INST_INT,
        c.FUTURE_INST_INT_OVRD_DBT,
        c.FUTURE_INST_OVRD_DBT,
        c.MARKETING_INFO,
        c.NO_OF_SUB_CARD,
        c.OSTND_INT_AMT,
        c.OSTND_PRIN_AMT,
        c.OVRD_INT_AMT,
        c.OVRD_INT_PEN_AMT,
        c.OVRD_PRIN_AMT,
        c.OVRD_PRIN_PEN_AMT,
        c.TOTAL_INT_AMT,
        c.TOTAL_PENALTY_AMT,
        c.CARD_CANCELLATION_DATE
    FROM   months m
    JOIN   CORE.CARD c
           ON  c.AS_OF_DT = m.MONTH_END
           AND c.CARD_TP  = 'P'
           AND c.DC       = 'C'
           AND c.CUST_TP  = 'F'
           AND c.status != 'R'
           AND ( c.CARD_CANCELLATION_DATE > DATE '2021-01-01'
                OR c.CARD_CANCELLATION_DATE IS NULL
                )
           AND ( c.CARD_CANCELLATION_DATE IS NULL
                OR EXISTS (
                        SELECT 1
                        FROM   ANALYTICS.CANCELLATION_REQUESTS r
                        WHERE  r.CARD_ID = c.CARD_ID
                        AND    r.CARD_CANCELLATION_DATE = c.CARD_CANCELLATION_DATE
                )
                )
    JOIN   EDW.LU_PRD_CARD lp
           ON  lp.PRODUCT_ID   = c.PRODUS_CARD_ID
           AND lp.PD_LVL_2_NM  = :PRODUCT_NAME
),
stmt_ranked AS (
    SELECT
        bc.CARD_ID,
        bc.MONTH_END,
        s.PRE_STMT_PYMNT_MIN_AMT,
        s.STMT_PYMNT_AMT,
        s.STMT_PYMNT_MIN_AMT,
        s.STMT_REVOLVING_AMT,
        ROW_NUMBER() OVER (
            PARTITION BY bc.CARD_ID, bc.MONTH_END
            ORDER BY s.STMT_DT DESC
        ) AS rn
    FROM   card_snapshot bc
    LEFT JOIN DWH.CARD_STATEMENT s
           ON  s.CARD_ID  = bc.CARD_ID
           AND s.STMT_DT >= TRUNC(bc.MONTH_END, 'MM')
           AND s.STMT_DT <  ADD_MONTHS(TRUNC(bc.MONTH_END, 'MM'), 1)
),
stmt_agg AS (
    SELECT
        CARD_ID,
        MONTH_END,
        --SUM(PRE_STMT_PYMNT_AMT)                             AS PRE_STMT_PYMNT_AMT,
        SUM(PRE_STMT_PYMNT_MIN_AMT)                        AS PRE_STMT_PYMNT_MIN_AMT,
        SUM(STMT_PYMNT_AMT)                                AS STMT_PYMNT_AMT,
        SUM(STMT_PYMNT_MIN_AMT)                            AS STMT_PYMNT_MIN_AMT,
        MAX(CASE WHEN rn = 1 THEN STMT_REVOLVING_AMT END)  AS STMT_REVOLVING_AMT
    FROM   stmt_ranked
    GROUP BY CARD_ID, MONTH_END
),
stmt_detail_ranked AS (
    SELECT
        CARD_ID,
        TRUNC(TRX_DATE, 'MM') AS TRX_MONTH,
        FEE_AMOUNT,
        REMAINING_INSTALLMENT_AMOUNT,
        REMAINING_INSTALLMENT_COUNT,
        TOTAL_INSTALLMENT_AMOUNT,
        TOTAL_INSTALLMENT_COUNT,
        --TRX_AMT_RON,
        ROW_NUMBER() OVER (
            PARTITION BY CARD_ID, TRUNC(TRX_DATE, 'MM')
            ORDER BY TRX_DATE DESC
        ) AS rn
    FROM   CORE.CARD_STATEMENT_DETAIL
    WHERE  TRX_DATE >= :START_DATE
),
stmt_detail_agg AS (
    SELECT
        CARD_ID,
        TRX_MONTH,
        SUM(FEE_AMOUNT)                                     AS FEE_AMOUNT,
        MAX(CASE WHEN rn = 1 THEN REMAINING_INSTALLMENT_AMOUNT END) AS REMAINING_INSTALLMENT_AMOUNT,
        MAX(CASE WHEN rn = 1 THEN REMAINING_INSTALLMENT_COUNT END)  AS REMAINING_INSTALLMENT_COUNT,
        SUM(TOTAL_INSTALLMENT_AMOUNT)                       AS TOTAL_INSTALLMENT_AMOUNT,
        SUM(TOTAL_INSTALLMENT_COUNT)                        AS TOTAL_INSTALLMENT_COUNT
        --SUM(TRX_AMT_RON)                                    AS TRX_AMT_RON
    FROM   stmt_detail_ranked
    GROUP BY CARD_ID, TRX_MONTH
),
stmt_monthly AS (
    SELECT
        bc.CARD_ID,
        TRUNC(bc.MONTH_END, 'MM') AS TRX_MONTH,
        sa.PRE_STMT_PYMNT_MIN_AMT,
        sa.STMT_PYMNT_AMT,
        sa.STMT_PYMNT_MIN_AMT,
        sa.STMT_REVOLVING_AMT,
        sd.FEE_AMOUNT,
        sd.REMAINING_INSTALLMENT_AMOUNT,
        sd.REMAINING_INSTALLMENT_COUNT,
        sd.TOTAL_INSTALLMENT_AMOUNT,
        sd.TOTAL_INSTALLMENT_COUNT
        --sd.TRX_AMT_RON
    FROM   card_snapshot bc
    LEFT JOIN stmt_agg sa
           ON sa.CARD_ID   = bc.CARD_ID
          AND sa.MONTH_END = bc.MONTH_END
    LEFT JOIN stmt_detail_agg sd
           ON sd.CARD_ID   = bc.CARD_ID
          AND sd.TRX_MONTH = TRUNC(bc.MONTH_END, 'MM')
),
profit_monthly AS (
    SELECT
        CARD_ID,
        AS_OF_MONTH,
        TOTAL_PROFIT,
        MERCHANT_COMMISSION,
        INSURANCE_COMMISSION,
        FEE AS PROFIT_FEE,
        LOAN_NET_INTEREST_INCOME,
        IMPAIRMENT_LOSSES,
        CASH_ADVANCE_COMMISSION
    FROM   CORE.PROFIT_CARD_CC_MONTHLY
    WHERE  AS_OF_MONTH >= :START_YYYYMM
),
tlmk_monthly AS (
    SELECT
        bc.CARD_ID,
        TRUNC(bc.MONTH_END, 'MM') AS ISSUE_MONTH,
        COUNT(iss.ISSUE_NUMBER) AS NUMBER_OF_ISSUES
    FROM   card_snapshot bc
    LEFT JOIN CORE.ISSUE iss
           ON iss.CARD_ID = bc.CARD_ID
          AND TRUNC(iss.ISSUE_CREATEDATE, 'MM') = TRUNC(bc.MONTH_END, 'MM')
    GROUP BY
        bc.CARD_ID,
        TRUNC(bc.MONTH_END, 'MM')
),
trx_monthly AS (
    SELECT
        t.CARD_ID,
        TRUNC(t.VALUE_DT, 'MM')  AS TRX_MONTH,
        COUNT(*)                  AS TRX_CNT_MTH,
        SUM(t.TRX_AMT)           AS TRX_AMT_MTH,
        CASE WHEN COUNT(*) > 0
             THEN SUM(t.TRX_AMT) / COUNT(*)
             ELSE 0
        END                       AS AVG_TICKET_MTH,
        MEDIAN(t.TRX_AMT)        AS MEDIAN_TICKET_MTH
    FROM   DWH.CARD_TRX_ISSUING t
    WHERE  t.VALUE_DT >= :START_DATE
    GROUP BY t.CARD_ID, TRUNC(t.VALUE_DT, 'MM')
),
mcc_freq_ranked AS (
    SELECT
        t.CARD_ID,
        TRUNC(t.VALUE_DT, 'MM')  AS TRX_MONTH,
        t.MERCHANT_CATEG_CODE,
        ROW_NUMBER() OVER (
            PARTITION BY t.CARD_ID, TRUNC(t.VALUE_DT, 'MM')
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM   DWH.CARD_TRX_ISSUING t
    WHERE  t.VALUE_DT >= :START_DATE
    GROUP BY t.CARD_ID, TRUNC(t.VALUE_DT, 'MM'), t.MERCHANT_CATEG_CODE
),
mcc_spent_ranked AS (
    SELECT
        t.CARD_ID,
        TRUNC(t.VALUE_DT, 'MM')  AS TRX_MONTH,
        t.MERCHANT_CATEG_CODE,
        ROW_NUMBER() OVER (
            PARTITION BY t.CARD_ID, TRUNC(t.VALUE_DT, 'MM')
            ORDER BY SUM(t.TRX_AMT) DESC
        ) AS rn
    FROM   DWH.CARD_TRX_ISSUING t
    WHERE  t.VALUE_DT >= :START_DATE
    GROUP BY t.CARD_ID, TRUNC(t.VALUE_DT, 'MM'), t.MERCHANT_CATEG_CODE
)
SELECT
    bc.CARD_ID,
    bc.MONTH_END,
    cs.BRCH_CODE,
    cs.STATUS,
    cs.CARD_LIMIT,
    cs.TOTAL_EXPOSURE_AMT,
    cs.OVERDUE_AMT_RON,
    cs.FUTURE_INST_AMT,
    cs.BONUS_AMT,
    cs.DPD,
    cs.TIMES_IN_5_DPD,
    cs.TIMES_IN_30_DPD,
    cs.TIMES_IN_60_DPD,
    cs.TIMES_IN_90_DPD,
    cs.TIMES_IN_90_PLUS_DPD,
    cs.LAST_EXPOSURE_DATE,
    cs.SENT_TO_LEGAL,
    cs.DOD,
    cs.POOL_RT,
    cs.ILOE,
    cs.DOUBTFUL_INT_AMT,
    cs.DOUBTFUL_INT_PEN_AMT,
    cs.DOUBTFUL_PRIN_AMT,
    cs.DOUBTFUL_PRIN_PEN_AMT,
    cs.FUTURE_INST_INT,
    cs.FUTURE_INST_INT_OVRD_DBT,
    cs.FUTURE_INST_OVRD_DBT,
    cs.MARKETING_INFO,
    cs.NO_OF_SUB_CARD,
    cs.OSTND_INT_AMT,
    cs.OSTND_PRIN_AMT,
    cs.OVRD_INT_AMT,
    cs.OVRD_INT_PEN_AMT,
    cs.OVRD_PRIN_AMT,
    cs.OVRD_PRIN_PEN_AMT,
    cs.TOTAL_INT_AMT,
    cs.TOTAL_PENALTY_AMT,
    cs.CARD_CANCELLATION_DATE,
    cu.CNTRY,
    cu.CITY,
    cu.EDUCATION,
    cu.RISK_LEVEL,
    cu.ADVERTISING,
    cu.BIRTH_DT, -- Add to validation
    cu.OPEN_DT, -- Add to validation
    sm.PRE_STMT_PYMNT_MIN_AMT,
    sm.STMT_PYMNT_AMT,
    sm.STMT_PYMNT_MIN_AMT,
    sm.STMT_REVOLVING_AMT,
    sm.FEE_AMOUNT,
    sm.REMAINING_INSTALLMENT_AMOUNT,
    sm.REMAINING_INSTALLMENT_COUNT,
    sm.TOTAL_INSTALLMENT_AMOUNT,
    sm.TOTAL_INSTALLMENT_COUNT,
    --sm.TRX_AMT_RON,
    pm.TOTAL_PROFIT,
    pm.MERCHANT_COMMISSION,
    pm.INSURANCE_COMMISSION,
    pm.PROFIT_FEE AS FEE,
    pm.LOAN_NET_INTEREST_INCOME,
    pm.IMPAIRMENT_LOSSES,
    pm.CASH_ADVANCE_COMMISSION,
    tm.TRX_CNT_MTH,
    tm.AVG_TICKET_MTH,
    tm.MEDIAN_TICKET_MTH,
    tm.TRX_AMT_MTH,
    tlm.NUMBER_OF_ISSUES,
    mf.MERCHANT_CATEG_CODE       AS MCC_MOST_FREQUENT_MTH,
    ms.MERCHANT_CATEG_CODE       AS MCC_MOST_SPENT_MTH
FROM      card_snapshot bc
LEFT JOIN card_snapshot cs
       ON cs.CARD_ID   = bc.CARD_ID
      AND cs.MONTH_END = bc.MONTH_END
LEFT JOIN CORE.CUSTOMER cu
       ON cu.CUST_ID = cs.CUST_ID
LEFT JOIN stmt_monthly sm
       ON sm.CARD_ID   = bc.CARD_ID
      AND sm.TRX_MONTH = TRUNC(bc.MONTH_END, 'MM')
LEFT JOIN profit_monthly pm
       ON pm.CARD_ID = bc.CARD_ID
      AND pm.AS_OF_MONTH =
             EXTRACT(YEAR FROM bc.MONTH_END) * 100 + EXTRACT(MONTH FROM bc.MONTH_END)
LEFT JOIN trx_monthly tm
       ON tm.CARD_ID   = bc.CARD_ID
      AND tm.TRX_MONTH = TRUNC(bc.MONTH_END, 'MM')
LEFT JOIN tlmk_monthly tlm
       ON tlm.CARD_ID   = bc.CARD_ID
      AND tlm.ISSUE_MONTH = TRUNC(bc.MONTH_END, 'MM')
LEFT JOIN mcc_freq_ranked mf
       ON mf.CARD_ID   = bc.CARD_ID
      AND mf.TRX_MONTH = TRUNC(bc.MONTH_END, 'MM')
      AND mf.rn        = 1
LEFT JOIN mcc_spent_ranked ms
       ON ms.CARD_ID   = bc.CARD_ID
      AND ms.TRX_MONTH = TRUNC(bc.MONTH_END, 'MM')
      AND ms.rn        = 1
 
 
 
