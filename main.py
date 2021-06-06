import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import xml.etree.ElementTree as ET

def main():

    gc = gspread.service_account(filename='client_secret.json')
    sh = gc.open_by_key('15TMn1ux2DLRydpZNceWBdgMHxqLN-k2Apv_f0hjd4B8')
    worksheet_chl = sh.get_worksheet(0)
    worksheet_col = sh.get_worksheet(1)
    worksheet_dnk = sh.get_worksheet(2)
    worksheet_eth = sh.get_worksheet(3)
    worksheet_irn = sh.get_worksheet(4)
    worksheet_jpn = sh.get_worksheet(5)
    
    main_indicators = ['GHO', 'COUNTRY', 'SEX', 'YEAR', 'GHECAUSES', 'AGEGROUP', 'Display', 'Numeric', 'Low', 'High']
    indicators = ['Number of deaths', 'Number of infant deaths', 'Number of under-five deaths', 
    'Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)', 
    'Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)', 
    'Estimates of number of homicides', 'Crude suicide rates (per 100 000 population)', 
    'Mortality rate attributed to unintentional poisoning (per 100 000 population)', 
    'Number of deaths attributed to non-communicable diseases, by type of disease and sex', 
    'Estimated road traffic death rate (per 100 000 population)',
    'Estimated number of road traffic deaths','Mean BMI (kg/m&#xb2;) (crude estimate)', 'Mean BMI (kg/m&#xb2;) (age-standardized estimate)',
    'Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)',  
    'Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)', 
    'Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)', 
    'Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)',
    'Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)', 
    'Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)',
    'Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)', 'Estimate of daily cigarette smoking prevalence (%)',
    'Estimate of daily tobacco smoking prevalence (%)', 'Estimate of current cigarette smoking prevalence (%)', 
    'Estimate of current tobacco smoking prevalence (%)','Mean systolic blood pressure (crude estimate)', 
    'Mean fasting blood glucose (mmol/l) (crude estimate)', 'Mean Total Cholesterol (crude estimate)']
    
    r_chl= requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_CHL.xml')
    root_chl = ET.fromstring(r_chl.content)
    chl_dict = {}
    i = 0
    for fact in root_chl:
        gho = fact.find('GHO').text
        if gho in indicators:
            country = fact.find('COUNTRY').text
            sex = fact.find('SEX').text
            year = fact.find('YEAR').text
            ghecauses = fact.find('GHECAUSES').text
            agegroup = fact.find('AGEGROUP').text
            display = fact.find('Display')
            if display is not None:
                display = display.text
            else:
                display= None
            numeric = fact.find('Numeric')
            if numeric is not None:
                numeric = numeric.text
            else:
                numeric= None
            low = fact.find('Low')
            if low is not None:
                low = low.text
            else:
                low = None
            high = fact.find('High')
            if high is not None:
                high = high.text
            else:
                high = None
            data = {'gho':gho, 'country':country, 'sex':sex, 'year':year, 'ghecauses':ghecauses, 
            'agegroup':agegroup, 'display':display, 'numeric': numeric, 'low': low, 'high':high}
            chl_dict[i] = data
            i += 1
    chl_dataframe = pd.DataFrame.from_dict(chl_dict, orient='index')
    
    r_col= requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_COL.xml')
    root_col = ET.fromstring(r_col.content)
    col_dict = {}
    i = 0
    for fact in root_col:
        gho = fact.find('GHO').text
        if gho in indicators:
            country = fact.find('COUNTRY').text
            sex = fact.find('SEX').text
            year = fact.find('YEAR').text
            ghecauses = fact.find('GHECAUSES').text
            agegroup = fact.find('AGEGROUP').text
            display = fact.find('Display')
            if display is not None:
                display = display.text
            else:
                display= None
            numeric = fact.find('Numeric')
            if numeric is not None:
                numeric = numeric.text
            else:
                numeric= None
            low = fact.find('Low')
            if low is not None:
                low = low.text
            else:
                low = None
            high = fact.find('High')
            if high is not None:
                high = high.text
            else:
                high = None
            data = {'gho':gho, 'country':country, 'sex':sex, 'year':year, 'ghecauses':ghecauses, 
            'agegroup':agegroup, 'display':display, 'numeric': numeric, 'low': low, 'high':high}
            col_dict[i] = data
            i += 1
    col_dataframe = pd.DataFrame.from_dict(col_dict, orient='index')
    
    r_dnk= requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_DNK.xml')
    root_dnk = ET.fromstring(r_dnk.content)
    dnk_dict = {}
    i = 0
    for fact in root_dnk:
        gho = fact.find('GHO').text
        if gho in indicators:
            country = fact.find('COUNTRY').text
            sex = fact.find('SEX').text
            year = fact.find('YEAR').text
            ghecauses = fact.find('GHECAUSES').text
            agegroup = fact.find('AGEGROUP').text
            display = fact.find('Display')
            if display is not None:
                display = display.text
            else:
                display= None
            numeric = fact.find('Numeric')
            if numeric is not None:
                numeric = numeric.text
            else:
                numeric= None
            low = fact.find('Low')
            if low is not None:
                low = low.text
            else:
                low = None
            high = fact.find('High')
            if high is not None:
                high = high.text
            else:
                high = None
            data = {'gho':gho, 'country':country, 'sex':sex, 'year':year, 'ghecauses':ghecauses, 
            'agegroup':agegroup, 'display':display, 'numeric': numeric, 'low': low, 'high':high}
            dnk_dict[i] = data
            i += 1
    dnk_dataframe = pd.DataFrame.from_dict(dnk_dict, orient='index')

    r_eth= requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_ETH.xml')
    root_eth = ET.fromstring(r_eth.content)
    eth_dict = {}
    i = 0
    for fact in root_eth:
        gho = fact.find('GHO').text
        if gho in indicators:
            country = fact.find('COUNTRY').text
            sex = fact.find('SEX').text
            year = fact.find('YEAR').text
            ghecauses = fact.find('GHECAUSES').text
            agegroup = fact.find('AGEGROUP').text
            display = fact.find('Display')
            if display is not None:
                display = display.text
            else:
                display= None
            numeric = fact.find('Numeric')
            if numeric is not None:
                numeric = numeric.text
            else:
                numeric= None
            low = fact.find('Low')
            if low is not None:
                low = low.text
            else:
                low = None
            high = fact.find('High')
            if high is not None:
                high = high.text
            else:
                high = None
            data = {'gho':gho, 'country':country, 'sex':sex, 'year':year, 'ghecauses':ghecauses, 
            'agegroup':agegroup, 'display':display, 'numeric': numeric, 'low': low, 'high':high}
            eth_dict[i] = data
            i += 1
    eth_dataframe = pd.DataFrame.from_dict(eth_dict, orient='index')

    r_irn= requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_IRN.xml')
    root_irn = ET.fromstring(r_irn.content)
    irn_dict = {}
    i = 0
    for fact in root_irn:
        gho = fact.find('GHO').text
        if gho in indicators:
            country = fact.find('COUNTRY').text
            sex = fact.find('SEX').text
            year = fact.find('YEAR').text
            ghecauses = fact.find('GHECAUSES').text
            agegroup = fact.find('AGEGROUP').text
            display = fact.find('Display')
            if display is not None:
                display = display.text
            else:
                display= None
            numeric = fact.find('Numeric')
            if numeric is not None:
                numeric = numeric.text
            else:
                numeric= None
            low = fact.find('Low')
            if low is not None:
                low = low.text
            else:
                low = None
            high = fact.find('High')
            if high is not None:
                high = high.text
            else:
                high = None
            data = {'gho':gho, 'country':country, 'sex':sex, 'year':year, 'ghecauses':ghecauses, 
            'agegroup':agegroup, 'display':display, 'numeric': numeric, 'low': low, 'high':high}
            irn_dict[i] = data
            i += 1
    irn_dataframe = pd.DataFrame.from_dict(irn_dict, orient='index')
    
    r_jpn= requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_JPN.xml')
    root_jpn = ET.fromstring(r_jpn.content)
    jpn_dict = {}
    i = 0
    for fact in root_jpn:
        gho = fact.find('GHO').text
        if gho in indicators:
            country = fact.find('COUNTRY').text
            sex = fact.find('SEX').text
            year = fact.find('YEAR').text
            ghecauses = fact.find('GHECAUSES').text
            agegroup = fact.find('AGEGROUP').text
            display = fact.find('Display')
            if display is not None:
                display = display.text
            else:
                display= None
            numeric = fact.find('Numeric')
            if numeric is not None:
                numeric = numeric.text
            else:
                numeric= None
            low = fact.find('Low')
            if low is not None:
                low = low.text
            else:
                low = None
            high = fact.find('High')
            if high is not None:
                high = high.text
            else:
                high = None
            data = {'gho':gho, 'country':country, 'sex':sex, 'year':year, 'ghecauses':ghecauses, 
            'agegroup':agegroup, 'display':display, 'numeric': numeric, 'low': low, 'high':high}
            jpn_dict[i] = data
            i += 1
    jpn_dataframe = pd.DataFrame.from_dict(jpn_dict, orient='index')

    set_with_dataframe(worksheet_chl, chl_dataframe)
    set_with_dataframe(worksheet_col, col_dataframe)
    set_with_dataframe(worksheet_dnk, dnk_dataframe)
    set_with_dataframe(worksheet_eth, eth_dataframe)
    set_with_dataframe(worksheet_irn, irn_dataframe)
    set_with_dataframe(worksheet_jpn, jpn_dataframe)

if __name__ == '__main__':
    main()
