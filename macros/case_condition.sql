{% macro case_condition_week_maxweek_dayrank_maxday_kpi(fyearweek, MaxFyearWeek, DayRank, MaxDay, kpi) %}
    CASE 
        WHEN {{fyearweek}} = ({{MaxFyearWeek}}-100) AND {{DayRank}} < {{MaxDay}}
            THEN {{kpi}}
        WHEN {{fyearweek}} <> ({{MaxFyearWeek}}-100) AND {{DayRank}} <= 7 
            THEN {{kpi}}
        ELSE 
            NULL 
    END
{% endmacro %}

{% macro case_condition_salesstatusid_week_maxweek_dayrank_maxday_kpi(salesstatusid,fyearweek, MaxFyearWeek, DayRank, MaxDay, kpi) %}
    CASE 
        WHEN {{salesstatusid}} <> 4 AND {{fyearweek}} = ({{MaxFyearWeek}}-100) AND {{DayRank}} < {{MaxDay}}
            THEN {{kpi}}
        WHEN {{salesstatusid}} <> 4 AND {{fyearweek}} <> ({{MaxFyearWeek}}-100) AND {{DayRank}} <= 7 
            THEN {{kpi}}
        ELSE 
            NULL
    END
{% endmacro %}

{% macro case_condition_isreturn_week_maxweek_dayrank_maxday_kpi(isreturn,fyearweek, MaxFyearWeek, DayRank, MaxDay, kpi) %}
    CASE 
        WHEN {{isreturn}} = 1 AND {{fyearweek}} = ({{MaxFyearWeek}}-100) AND {{DayRank}} < {{MaxDay}}
            THEN {{kpi}}
        WHEN {{isreturn}} = 1 AND {{fyearweek}} <> ({{MaxFyearWeek}}-100) AND {{DayRank}} <= 7 
            THEN {{kpi}}
        ELSE 
            NULL 
    END
{% endmacro %}