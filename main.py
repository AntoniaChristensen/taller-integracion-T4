import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import xml.etree.ElementTree as ET

def main():

    gc = gspread.service_account(filename='client_secret.json')
    sh = gc.open_by_key('15TMn1ux2DLRydpZNceWBdgMHxqLN-k2Apv_f0hjd4B8')
    worksheet = sh.get_worksheet(0)    
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
    countries = ['CHL', 'COL', 'DNK', 'ETH', 'IRN', 'JPN']
    dict_info = {}
    i = 0
    for country in countries:
        request = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{}.xml'.format(country))
        root = ET.fromstring(request.content)
        for fact in root:
            gho = fact.find('GHO').text
            if gho in indicators:
                display = fact.find('Display')
                if display is not None:
                    display = display.text
                else:
                    display= None
                numeric = fact.find('Numeric')
                if numeric is not None:
                    numeric = float(numeric.text)
                else:
                    numeric= None
                if display == '0' and numeric == 0:
                    next
                country = fact.find('COUNTRY').text
                sex = fact.find('SEX').text
                year = fact.find('YEAR').text
                ghecauses = fact.find('GHECAUSES').text
                agegroup = fact.find('AGEGROUP').text
                low = fact.find('Low')
                if low is not None:
                    low = float(low.text)
                else:
                    low = None
                high = fact.find('High')
                if high is not None:
                    high = float(high.text)
                else:
                    high = None
                data = {'gho':gho, 'country':country, 'sex':sex, 'year':year, 'ghecauses':ghecauses, 
                'agegroup':agegroup, 'display':display, 'numeric': numeric, 'low': low, 'high':high}
                dict_info[i] = data
                i += 1
        print('done pais: {}'.format(country))
    print("done for")
 
    dataframe = pd.DataFrame.from_dict(dict_info, orient='index')
    print('done dataframe')

    set_with_dataframe(worksheet, dataframe)
    print('done worksheet')

if __name__ == '__main__':
    main()
