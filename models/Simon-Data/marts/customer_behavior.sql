 {{
  config(
    materialized='table'
  )
}}

 with demo_all_customer_stg_de_dup as
 (
  select
  *
  from
  {{ ref("src_demo_all_customer_stg_de_dup") }}

 ),
  mfrm_segment_scores as (
  select
  *
  from
  {{ ref("src_mfrm_segment_scores") }}

 ),

  test as (
  select
    a.MasterCustCD as MasterCustCD,
    a.customer_value_segment as customer_value_segment,
    b.person_v12Consumer_dwelling_type as consumer_dwelling_type, --mapp the column
    b.person_v12Consumer_wealthrating as consumer_wealthrating,
    b.person_v12Consumer_vista_segment as consumer_vista_segment,
    b.person_v12Consumer_vista_type as consumer_vista_type,
    b.person_v12Consumer_gender as consumer_gender,
    b.person_v12Consumer_vincome as consumer_vincome,
    b.person_v12Consumer_vhomevalue as consumer_vhomevalue,
    b.person_v12Consumer_income as consumer_income,
    b.person_v12Consumer_net_worth as consumer_net_worth,
    b.person_v12Consumer_age as consumer_age,
    b.person_v12Consumer_race as consumer_race,
    b.person_v12Consumer_credit_ranges as consumer_credit_ranges,
    b.person_v12Consumer_occupation as consumer_occupation,
    b.person_v12Consumer_mail_responder as consumer_mail_responder,
    b.person_v12Consumer_marital_status as consumer_marital_status,
    b.person_v12Consumer_home_owner as consumer_home_owner,
    b.person_v12Consumer_num_of_children as consumer_num_of_children,
    b.person_v12Consumer_credit_card as consumer_credit_card,
    b.person_v12Consumer_home_price as consumer_home_price,
    b.person_v12Consumer_education_code as consumer_education_code,
    b.person_v12Consumer_number_of_bedrooms as consumer_number_of_bedrooms,
  from (
    select
      MasterCustCD,
      customer_value_segment
    from (
      select
        MasterCustCD,
        customer_value_segment,
        ROW_NUMBER() over (partition by sc.MasterCustCD order by SAFE_CAST(Scored_Date as date format 'DD/MM/YYY') desc) as rank_segment
      from
        mfrm_segment_scores sc )
    where
      rank_segment = 1) a
  join
    demo_all_customer_stg_de_dup b
  on
    a.MasterCustCD=b.MasterCustCD )
 
select
  test.mastercustcd,
  customer_value_segment,
  case
    when consumer_dwelling_type="A" then "Multi Family Dwelling/Apartment"
    when consumer_dwelling_type="P" then "PO Box"
    when consumer_dwelling_type="S" then "Single Family"
  else
  NULL
end
  as consumer_dwelling_type,
  case
    when consumer_wealthrating="1" then "10th decile (lowest)"
    when consumer_wealthrating="2" then "9th decile"
    when consumer_wealthrating="3" then "8th decile"
    when consumer_wealthrating="4" then "7th decile"
    when consumer_wealthrating="5" then "6th decile"
    when consumer_wealthrating="6" then "5th decile"
    when consumer_wealthrating="7" then "4th decile"
    when consumer_wealthrating="8" then "3rd decile"
    when consumer_wealthrating="9" then "2nd decile"
    when consumer_wealthrating="10" then "1st decile (top10%)"
  else
  NULL
end
  as consumer_wealthrating,
  case
    when LOWER(consumer_gender) ="f" then "Female"
    when LOWER(consumer_gender) ="m" then "Male"
  else
  NULL
end
  as consumer_gender,
  case when consumer_vista_segment  = 'A' then 'A: Rustic Families'
        when consumer_vista_segment  = 'B' then 'B: Thriving Suburbanites'
        when consumer_vista_segment  = 'C' then 'C: Autumn Years'
        when consumer_vista_segment  = 'D' then 'D: Young City Parents'
        when consumer_vista_segment  = 'E' then 'E: Prosperous with Property'
        when consumer_vista_segment  = 'F' then 'F: Singles in the City'
        when consumer_vista_segment  = 'G' then 'G: Booming with Confidence'
        when consumer_vista_segment  = 'H' then 'H: Rustic Homeowners'
        when consumer_vista_segment  = 'I' then 'I: City Boomers'
        when consumer_vista_segment  = 'J' then 'J: Upscale City Families'
        when consumer_vista_segment  = 'K' then 'K: Middle Income Suburban Dwellers'
        when consumer_vista_segment  = 'L' then 'L: Thrifty Consumers'
        when consumer_vista_segment  = 'M' then 'M: Mature & Practical Shoppers'
        when consumer_vista_segment  = 'N' then 'N: Renters on the Rise'
        when consumer_vista_segment  = 'O' then 'O: Mature Rustic Homeowners'
        when consumer_vista_segment  = 'P' then 'P: Millennial Suburban Homeowners'
        when consumer_vista_segment  = 'Q' then 'Q: Strapped for Cash'
        when consumer_vista_segment  = 'R' then 'R: Privacy Matters'
        when consumer_vista_segment  = 'S' then 'S: Privacy Matters Homeowners'
        when consumer_vista_segment  = 'T' then 'T: Affluent City Boomers'
        when consumer_vista_segment  = 'U' then 'U: Thrifty Boomers'
        when consumer_vista_segment  = 'V' then 'V: 40 Plus Home Lovers'
        else
         NULL
     end as consumer_vista_segment,

   case when consumer_vista_type = '01' then 'A: Healthy and Wealthy'
        when consumer_vista_type = '02' then 'B: At Home and In Bulk'
        when consumer_vista_type = '03' then 'C: Luxe and Levered'
        when consumer_vista_type = '04' then 'D: Thrifty Shoppers'
        when consumer_vista_type = '05' then 'E: Up For Grabs'
        else
         NULL
     end as consumer_vista_type,

  case
    when LOWER(consumer_vincome)="a" then "Under $10,000"
    when LOWER(consumer_vincome)="b" then "$10,000 - $14,999"
    when LOWER(consumer_vincome)="c" then "$15,000 - $19,999"
    when LOWER(consumer_vincome)="d" then "$20,000 - $24,999"
    when LOWER(consumer_vincome)="e" then "$25,000 - $29,999"
    when LOWER(consumer_vincome)="f" then "$30,000 - $34,999"
    when LOWER(consumer_vincome)="g" then "$35,000 - $39,999"
    when LOWER(consumer_vincome)="h" then "$40,000 - $44,999"
    when LOWER(consumer_vincome)="i" then "$45,000 - $49,999"
    when LOWER(consumer_vincome)="j" then "$50,000 - $54,999"
    when LOWER(consumer_vincome)="k" then "$55,000 - $59,999"
    when LOWER(consumer_vincome)="l" then "$60,000 - $64,999"
    when LOWER(consumer_vincome)="m" then "$65,000 - $74,999"
    when LOWER(consumer_vincome)="n" then "$75,000 - $99,999"
    when LOWER(consumer_vincome)="o" then "$100,000 - $149,999"
    when LOWER(consumer_vincome)="p" then "$150,000 - $174,999"
    when LOWER(consumer_vincome)="q" then "$175,000 - $199,999"
    when LOWER(consumer_vincome)="r" then "$200,000 - $249,999"
    when LOWER(consumer_vincome)="s" then "$250,000 +"
  else
  NULL
end
  as consumer_vincome,
  case
    when LOWER(consumer_vhomevalue)="a" then "$1,000 - $24,999"
    when LOWER(consumer_vhomevalue)="b" then "$25,000 - $49,999"
    when LOWER(consumer_vhomevalue)="c" then "$50,000 - $74,999"
    when LOWER(consumer_vhomevalue)="d" then "$75,000 - $99,999"
    when LOWER(consumer_vhomevalue)="e" then "$100,000 - $124,999"
    when LOWER(consumer_vhomevalue)="f" then "$125,000 - $149,999"
    when LOWER(consumer_vhomevalue)="g" then "$150,000 - $174,999"
    when LOWER(consumer_vhomevalue)="h" then "$175,000 - $199,999"
    when LOWER(consumer_vhomevalue)="i" then "$200,000 - $224,999"
    when LOWER(consumer_vhomevalue)="j" then "$225,000 - $249,999"
    when LOWER(consumer_vhomevalue)="k" then "$250,000 - $274,999"
    when LOWER(consumer_vhomevalue)="l" then "$275,000 - $299,999"
    when LOWER(consumer_vhomevalue)="m" then "$300,000 - $349,999"
    when LOWER(consumer_vhomevalue)="n" then "$350,000 - $399,999"
    when LOWER(consumer_vhomevalue)="o" then "$400,000 - $449,999"
    when LOWER(consumer_vhomevalue)="p" then "$450,000 - $499,999"
    when LOWER(consumer_vhomevalue)="q" then "$500,000 - $749,999"
    when LOWER(consumer_vhomevalue)="r" then "$750,000 - $999,999"
    when LOWER(consumer_vhomevalue)="s" then "$1,000,000 Plus"
  else
  NULL
end
  as consumer_vhomevalue,
  case
    when LOWER(consumer_income)="a" then "Under $10k"
    when LOWER(consumer_income)="b" then "10-19,999"
    when LOWER(consumer_income)="c" then "20-29,999"
    when LOWER(consumer_income)="d" then "30-39,999"
    when LOWER(consumer_income)="e" then "40-49,999"
    when LOWER(consumer_income)="f" then "50-59,999"
    when LOWER(consumer_income)="g" then "60-69,999"
    when LOWER(consumer_income)="h" then "70-79,999"
    when LOWER(consumer_income)="i" then "80-89,999"
    when LOWER(consumer_income)="j" then "90-99,999"
    when LOWER(consumer_income)="k" then "100-149,999"
    when LOWER(consumer_income)="l" then "150 - 174,999"
    when LOWER(consumer_income)="m" then "175 - 199,999"
    when LOWER(consumer_income)="n" then "200 - 249,999"
    when LOWER(consumer_income)="o" then "250k+"
  else
  NULL
end
  as consumer_income,
  case
    when LOWER(consumer_net_worth)="a" then "Less than $1"
    when LOWER(consumer_net_worth)="b" then "$1 - $4,999"
    when LOWER(consumer_net_worth)="c" then "$5,000 - $9,999"
    when LOWER(consumer_net_worth)="d" then "$10,000 - $24,999"
    when LOWER(consumer_net_worth)="e" then "$25,000 - $49,999"
    when LOWER(consumer_net_worth)="f" then "$50,000 - $99,999"
    when LOWER(consumer_net_worth)="g" then "$100,000 - $249,999"
    when LOWER(consumer_net_worth)="h" then "$250,000 - $499,999"
    when LOWER(consumer_net_worth)="i" then "Greater than $499,999"
    when LOWER(consumer_net_worth)="j" then "90-99,999"
    when LOWER(consumer_net_worth)="k" then "100-149,999"
    when LOWER(consumer_net_worth)="l" then "150 - 174,999"
    when LOWER(consumer_net_worth)="m" then "175 - 199,999"
    when LOWER(consumer_net_worth)="n" then "200 - 249,999"
    when LOWER(consumer_net_worth)="o" then "250k+"
  else
  NULL
end
  as consumer_net_worth,
  consumer_age,
  case
    when consumer_race="A1" then "asian - Unknown"
    when consumer_race="A2" then "asian - Chinese"
    when consumer_race="A3" then "asian - Indian"
    when consumer_race="A4" then "asian - Japanese"
    when consumer_race="A5" then "asian - Oriental"
    when consumer_race="B1" then "African American - African Origin"
    when consumer_race="B2" then "African American - American Origin"
    when consumer_race="H1" then "Hispanic - Hispanic Origin"
    when consumer_race="H2" then "Hispanic - Portuguese Origin"
    when consumer_race="I1" then "American Indian"
	  when consumer_race="M1" then "Middle Eastern - Arab"
    when consumer_race="M2" then "Middle Eastern - Egyptian"
	  when consumer_race="W1" then "Caucasian / White - English"
    when consumer_race="W2" then "Caucasian / White - European"
    when consumer_race="W3" then "Caucasian / White - White Non-American"
    when consumer_race="W4" then "Caucasian / White - Unknown"
    when consumer_race="W5" then "Caucasian / White - Eastern European"
    when consumer_race="W6" then "Caucasian / White - Jewish"
    when consumer_race="W7" then "Caucasian / White - Greek"
    when consumer_race="W8" then "Caucasian / White - Dutch"
	else
  NULL
end
  as consumer_race,
  case
    when LOWER(consumer_credit_ranges)="a" then "800+"
    when LOWER(consumer_credit_ranges)="b" then "750-799 "
    when LOWER(consumer_credit_ranges)="c" then "700-749"
    when LOWER(consumer_credit_ranges)="d" then "650-699"
    when LOWER(consumer_credit_ranges)="e" then "600-649"
    when LOWER(consumer_credit_ranges)="f" then "550-599"
    when LOWER(consumer_credit_ranges)="g" then "500-549"
    when LOWER(consumer_credit_ranges)="h" then "499 & less"
  else
  NULL
end
  as consumer_credit_ranges,
  case
    when consumer_occupation="1" then "Homemaker"
    when consumer_occupation="2" then "Professional/Technical"
    when consumer_occupation ="3" then "Upper management/Executive"
    when consumer_occupation ="4" then "Middle management"
    when consumer_occupation ="5" then "Sales/Marketing"
    when consumer_occupation ="6" then "Clerical or Service worker"
    when consumer_occupation ="7" then "Tradesman/Machine/Laborer"
    when consumer_occupation="8" then "Retired"
    when consumer_occupation="9" then "Student"
    when consumer_occupation="10" then "Executive/Administrator"
    when consumer_occupation="11" then "Self employed"
    when consumer_occupation="12" then "Professional driver"
    when consumer_occupation="13" then "Military"
    when consumer_occupation="14" then "Civil servant"
    when consumer_occupation="15" then "Farming/Agriculture"
    when consumer_occupation="16" then "Work from home"
    when consumer_occupation="17" then "Health services"
    when consumer_occupation="18" then "Financial services"
    when consumer_occupation="20" then "Business owner"
    when consumer_occupation="21" then "Teacher/educator"
    when consumer_occupation="22" then "Retail sales"
    when consumer_occupation="23" then "Computer professional"
    when consumer_occupation="30" then "Beauty (cosmetologist, barber, manicurist, nails)"
    when consumer_occupation="31" then "Real estate (sales, brokers, appraisers)"
    when consumer_occupation="32" then "Architects"
    when consumer_occupation="33" then "Interior designers"
    when consumer_occupation="34" then "Landscape architects"
    when consumer_occupation="35" then "Electricians"
    when consumer_occupation="36" then "Engineers"
    when consumer_occupation="37" then "Accountants"
    when consumer_occupation="38" then "Attorneys"
    when consumer_occupation="39" then "Aocial worker"
    when consumer_occupation="40" then "Counselors"
    when consumer_occupation="41" then "Occupational therapist/Physical therapist"
    when consumer_occupation="42" then "Speech pathologistaudiologist"
    when consumer_occupation="43" then "Psychologists"
    when consumer_occupation="44" then "Pharmacist"
    when consumer_occupation="45" then "Opticians/Optometrist"
    when consumer_occupation="46" then "Veterinarian"
    when consumer_occupation="47" then "Dentist/Dental hygienist"
    when consumer_occupation="48" then "Nurses"
    when consumer_occupation="49" then "Doctors/Physicians/Surgeons"
    when consumer_occupation="50" then "Chiropractors"
    when consumer_occupation="51" then "Surveyors"
    when consumer_occupation="52" then "Disabled"
    when lower(consumer_occupation)="blank" then "Unknown"
  else
  NULL
end
  as consumer_occupation,
  case
    when consumer_mail_responder="1" then "Indicating a response/Inquiry was made by mail"
  else
  NULL
end
  as consumer_mail_responder,
  case
    when LOWER(consumer_marital_status)="m" then "Married"
    when LOWER(consumer_marital_status)="s" then "Single"
  else
  NULL
end
  as consumer_marital_status,
  case
    when LOWER(consumer_home_owner)="p" then "Probable renter"
    when LOWER(consumer_home_owner)="r" then "Renter"
    when LOWER(consumer_home_owner)="y" then "Home owner"
    when LOWER(consumer_home_owner)="w" then "Probable home owner"
  else
  NULL
end
  as consumer_home_owner,
  case
    when consumer_num_of_children="2" then "Two children"
    when consumer_num_of_children="1" then "One child"
    when consumer_num_of_children="3" then "Three children"
    when consumer_num_of_children="4" then "Four children"
    when consumer_num_of_children="0" then "No children"
    when consumer_num_of_children="5" then "Five children"
    when consumer_num_of_children="6" then "Six children"
    when consumer_num_of_children="7" then "Seven children"
    when consumer_num_of_children="8" then "Eight children"
  else
  NULL
end
  as consumer_num_of_children,
  case
    when consumer_credit_card="1" then "Owns a credit card"
  else
  NULL
end
  as consumer_credit_card,
  consumer_home_price,
  case
    when consumer_education_code="1" then "High school"
    when consumer_education_code="2" then "Some college"
    when consumer_education_code="3" then "Completed college"
    when consumer_education_code="4" then "Graduate school"
  else
  NULL
end
  as consumer_education_code,
  consumer_number_of_bedrooms,
  cps.PropensityPredictedTargetBinaryScores as predicted_propensity_12months,
  cps.SpendPredictedTargetSales as predicted_spend_12months,
  NTILE(10) OVER (ORDER BY cps.PropensityPredictedTargetBinaryScores desc) AS predicted_propensity_12months_decile,
  NTILE(10) OVER (ORDER BY cps.SpendPredictedTargetSales desc) AS predicted_spend_12months_decile

from
  test
left join
  {{ref('stg_clv_propensity_spend')}} as cps
  on cps.MasterCustCD = test.MasterCustCD