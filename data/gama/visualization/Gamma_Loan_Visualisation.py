import sys
import numpy as np 
import pandas as pd 
import folium
from folium import plugins
from folium.plugins import Search
from functools import reduce
from glob import glob
from datetime import datetime
import math

# read published gamma-loan data
print("######################################")
print("READING PUBLISHED GAMMA-LOAN DATA FILE")
print("######################################")

dtStr = datetime.today().strftime('%Y%m%d')
file = glob('/usr/local/spark/resources/data/published/enriched_loan_book/dt='+dtStr+'/part*.csv')[0]
#file = glob('/usr/local/spark/resources/data/published/enriched_loan_book/dt=20230112/part*.csv')[0]
df_gamma_loan = pd.read_csv(file)

#Sligo co-ordinates
lat= 54.26969
lang= -8.46943

#Color codes for BER rating scale
colors_ber={
    'A1':'#00a54f',
    'A2':'#00a54f',
    'A3':'#00a54f',
    'B1':'#4cb848',
    'B2':'#4cb848',
    'B3':'#4cb848',
    'C1':'#bed630',
    'C2':'#bed630',
    'C3':'#bed630',
    'D1':'#fff101',
    'D2':'#fff101',
    'E1':'#fcb814',
    'E2':'#fcb814',
    'F':'#f36e21',
    'G':'#ee1d23',
}

#Color codes for flood rating scale
colors_flood={
    'No Colour':'#deebf6',
    'green':'#a5d45d',
    'amber':'#ecc63a',
    'red':'#d92424',
    'black 1':'#919191',
    'black 2':'#242424',
}

# Creating the popup labels for energy rating data
def popup_html_ber(row):
    i = row
    ecad_id=df_gamma_loan['ecad_id'].iloc[i] 
    Address= df_gamma_loan['address_line_1'].iloc[i] +' '+ df_gamma_loan['address_line_2'].iloc[i] +' '+ df_gamma_loan['address_line_3'].iloc[i]
    Ber_Rating = df_gamma_loan['ber_rating'].iloc[i]
    Ber_Rating_Kwh = df_gamma_loan['ber_rating_kwh'].iloc[i]
    Co2_Emission = df_gamma_loan['co2_emission'].iloc[i]

    left_col_color = "#808080"
    right_col_color = "#dcdcdc"
    
    html = """
    <!DOCTYPE html>
    <html>
    <center><h4 style="margin-bottom:5"; width="200px">ECAD ID:{}</h4>""".format(ecad_id) + """</center> 
    <center> <table style="height: 126px; width: 305px;">
    <tbody>
    <tr> 
    <td style="background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Address </span></td>
    <td style="width: 150px;background-color: """+ right_col_color +""";">"""+ Address + """</td>
    </tr>
    <tr>
    <td style="background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Ber Rating </span></td>
    <td style="width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(Ber_Rating) + """
    </tr>
    <tr>
    <td style="background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Ber Rating KWh </span></td>
    <td style="width: 150px;background-color: """+ right_col_color +""";">{} kWh/m\u00b2/year</td>""".format(Ber_Rating_Kwh) + """
    </tr>
    <tr>
    <td style="background-color: """+ left_col_color +""";"><span style="color: #ffffff;">CO\u2082 Emission </span></td>
    <td style="width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(Co2_Emission) + """
    </tr>
    </tbody>
    </table></center>
    </html>
"""
    return html
    
# Creating the popup labels for flood rating data
def popup_html_flood(row):
    i = row
    ecad_id=df_gamma_loan['ecad_id'].iloc[i] 
    address= df_gamma_loan['address_line_1'].iloc[i] +' '+ df_gamma_loan['address_line_2'].iloc[i] +' '+ df_gamma_loan['address_line_3'].iloc[i]
    river_flooding_first = df_gamma_loan['riverrp'].iloc[i]
    coastal_flooding_first = df_gamma_loan['coastaludrp'].iloc[i]
    surf_water_flooding_first = df_gamma_loan['swaterrp'].iloc[i]
    
    left_col_color = "#808080"
    right_col_color = "#dcdcdc"
    
    html = """
    <!DOCTYPE html>
    <html>
    <center><h4 style="margin-bottom:5"; width="200px">ECAD ID:{}</h4>""".format(ecad_id) + """</center>
    <center> <table style="height: 130px; width: 500px;">
    <tbody>
    <tr>
    <td style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Address </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">"""+ address + """</td>
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Return Period at which River Flooding First Occurred </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(river_flooding_first) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Return Period at which Coastal Flooding First Occurred  </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(coastal_flooding_first) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Return Period at which River Surface Water Flooding First Occurred  </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(surf_water_flooding_first) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Combined Score - Undefended River, Coastal and Surface Water</span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['floodscore_ud'].iloc[i]) + """
    </tr>
    </tbody>
    </table>
    </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">River</h5>
    <center> <table style="height: 130px; width: 500px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">20 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">75 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">100 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">200 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">1000 years </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style= "border: 1px solid white; width=100px; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Max. Depth of Flooding in the River Undefended in Meters </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['rmax20'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['rmax75'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['rmax100'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['rmax200'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['rmax1000'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">River Undefended Flood Score </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['r20matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['r75matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['r100matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['r200matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['r1000matrix'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Property Modelled for Undefended River Flood </span></td>
    <td colspan="5" style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['model_river'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Coastal</h5>
    <center> <table style="height: 130px; width: 500px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">75 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">100 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">200 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">1000 years </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style= "border: 1px solid white; width=100px; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Max. Depth of Flooding in the Coastal Undefended in Meters </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cudmax75'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cudmax100'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cudmax200'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cudmax1000'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Coastal Undefended Flood Score </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cud75matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cud100matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cud200matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cud1000matrix'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Property Modelled for Coastal Flood </span></td>
    <td colspan="4" style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['model_coastal'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Surface Water</h5>
    <center> <table style="height: 130px; width: 500px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">75 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">200 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">1000 years </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style= "border: 1px solid white; width=100px; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Max. Depth of Flooding in the Surface Water Undefended in Meters </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['swmax75'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['swmax200'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['swmax1000'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Surface Water Undefended Flood Score </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['sw75matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['sw200matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['sw1000matrix'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Property Modelled for Surface Water Flood </span></td>
    <td colspan="4" style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['model_sw'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    </html>
""" 
    return html

# Creating the popup labels for gamma and loan data
def popup_html_index(row):
    i = row
    ecad_id=df_gamma_loan['ecad_id'].iloc[i] 
    address= df_gamma_loan['address_line_1'].iloc[i] +' '+ df_gamma_loan['address_line_2'].iloc[i] +' '+ df_gamma_loan['address_line_3'].iloc[i]
    river_flooding_first = df_gamma_loan['riverrp'].iloc[i]
    coastal_flooding_first = df_gamma_loan['coastaludrp'].iloc[i]
    surf_water_flooding_first = df_gamma_loan['swaterrp'].iloc[i]
    
    left_col_color = "#808080"
    right_col_color = "#dcdcdc"
    
    html = """
    <!DOCTYPE html>
    <html>
    <center><h4 style="margin-bottom:5"; width="200px">ECAD ID:{}</h4>""".format(ecad_id) + """</center>
    <center> <table style="height: 130px; width: 600px;">
    <tbody>
    <tr>
    <td style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Address </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">"""+ address + """</td>
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">BER Rating </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{} ({} kWh/m\u00b2/year)</td>""".format(df_gamma_loan['ber_rating'].iloc[i],df_gamma_loan['ber_rating_kwh'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">CO\u2082 Emission</span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['co2_emission'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Return Period at which River Flooding First Occurred </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(river_flooding_first) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Return Period at which Coastal Flooding First Occurred  </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(coastal_flooding_first) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Return Period at which River Surface Water Flooding First Occurred  </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(surf_water_flooding_first) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Combined Score - Undefended River, Coastal and Surface Water</span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['floodscore_ud'].iloc[i]) + """
    </tr>
    </tbody>
    </table>
    </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">River</h5>
    <center> <table style="height: 130px; width: 600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">20 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">75 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">100 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">200 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">1000 years </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style= "border: 1px solid white; width=100px; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Max. Depth of Flooding in the River Undefended in Meters </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['rmax20'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['rmax75'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['rmax100'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['rmax200'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['rmax1000'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">River Undefended Flood Score </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['r20matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['r75matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['r100matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['r200matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['r1000matrix'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Coastal</h5>
    <center> <table style="height: 130px; width: 600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">75 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">100 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">200 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">1000 years </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style= "border: 1px solid white; width=100px; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Max. Depth of Flooding in the Coastal Undefended in Meters </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cudmax75'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cudmax100'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cudmax200'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cudmax1000'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Coastal Undefended Flood Score </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cud75matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cud100matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cud200matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['cud1000matrix'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Surface Water</h5>
    <center> <table style="height: 130px; width: 600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">75 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">200 years </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">1000 years </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style= "border: 1px solid white; width=100px; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Max. Depth of Flooding in the Surface Water Undefended in Meters </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['swmax75'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['swmax200'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['swmax1000'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Surface Water Undefended Flood Score </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['sw75matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['sw200matrix'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['sw1000matrix'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Loan Details</h5>
    <center> <table style="height: 130px; width:600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Asset_id</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Program/product</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Term</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Rate/margin</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Disbursment Date</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Completion Date</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Initial Borrow capital</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Loan to Value</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Monthly repayment</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Mortgage Outstanding Balance</span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['Asset_id'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['product'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['term'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}%</td>""".format(round(df_gamma_loan['Rate/Margin'].iloc[i]*100,2)) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['Disbursment_Date'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_gamma_loan['Closing_Date'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">€{}</td>""".format(df_gamma_loan['Initial_Borrow_Capital'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}%</td>""".format(math.floor(df_gamma_loan['LTV'].iloc[i]*100)) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">€{}</td>""".format(df_gamma_loan['Monthly_Repayment'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">€{}</td>""".format(df_gamma_loan['Mortgage_Outstanding'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    </html>
""" 
    return html
    
#Creating lagends
def add_legend(maps, title, colors, labels):
    if len(colors) != len(labels):
        raise ValueError("colors and labels must have the same length.")

    color_label = dict(zip(labels, colors))
    
    legends = ""     
    for label, color in color_label.items():
        legends += f"<li><span style='background:{color}'></span>{label}</li>"
        
    legend_html = f"""
    <div id='legend' class='legend'>
      <div class='legend-title'>{title}</div>
      <div class='legend-scale'>
        <ul class='legend-labels'>
        {legends}
        </ul>
      </div>
    </div>
    """
    script = f"""
        <script type="text/javascript">
        var Layout = (function() {{
                    var flag = false;
                    return function() {{
                        if (!flag) {{
                             var checkExist = setInterval(function() {{
                                       if ((document.getElementsByClassName('leaflet-top leaflet-right').length) || (!flag)) {{
                                          document.getElementsByClassName('leaflet-top leaflet-right')[0].style.display = "flex"
                                          document.getElementsByClassName('leaflet-top leaflet-right')[0].style.flexDirection = "column"
                                          document.getElementsByClassName('leaflet-top leaflet-right')[0].innerHTML += `{legend_html}`;
                                          clearInterval(checkExist);
                                          flag = true;
                                       }}
                                    }}, 100);
                        }}
                    }};
                }})();
        Layout()
        </script>
      """
   

    css = """

    <style type='text/css'>
      .legend {
        z-index:9999;
        float:right;
        background-color: rgb(255, 255, 255);
        border-radius: 5px;
        border: 2px solid #bbb;
        padding: 10px;
        font-size:12px;
        positon: relative;
      }
      .legend .legend-title {
        text-align: left;
        margin-bottom: 5px;
        font-weight: bold;
        font-size: 90%;
        }
      .legend .legend-scale ul {
        margin: 0;
        margin-bottom: 5px;
        padding: 0;
        float: left;
        list-style: none;
        }
      .legend .legend-scale ul li {
        font-size: 80%;
        list-style: none;
        margin-left: 0;
        line-height: 18px;
        margin-bottom: 2px;
        }
      .legend ul.legend-labels li span {
        display: block;
        float: left;
        height: 16px;
        width: 30px;
        margin-right: 5px;
        margin-left: 0;
        border: 0px solid #ccc;
        }
      .legend .legend-source {
        font-size: 80%;
        color: #777;
        clear: both;
        }
      .legend a {
        color: #777;
        }
    </style>
    """

    maps.get_root().header.add_child(folium.Element(script + css))

    return maps
 
#Plotting BER_rating data
print("\n------")
print("GENERATING BER RATING MAP")
print("------")

sligo_ber=folium.Map(location=[lat,lang],zoom_start=10)
for d in df_gamma_loan.iterrows():
        html = popup_html_ber(d[0])
        popup = folium.Popup(folium.Html(html, script=True), max_width=500)
        folium.CircleMarker(
                    [d[1]["etrs89_lat"], d[1]["etrs89_long"]],
                    radius=6,
                    color=colors_ber[d[1]["ber_rating"]],
                    fill=True,
                    fill_color=colors_ber[d[1]["ber_rating"]],
                    fill_opacity=0.7,
                    popup=popup
            ).add_to(sligo_ber)

sligo_ber = add_legend(sligo_ber, 'Building Energy Rating', colors = list(colors_ber.values()), labels = list(colors_ber.keys()))
sligo_ber.save('/usr/local/spark/resources/data/visualization/Sligo_Energy_Rating.html')
print("BER RATING MAP GENERATED AND SAVED")

#Plotting flood_rating data
print("\n------")
print("GENERATING FLOOD RATING MAP")
print("------")

sligo_flood=folium.Map(location=[lat,lang],zoom_start=10)
for d in df_gamma_loan.iterrows():
        html = popup_html_flood(d[0])
        popup = folium.Popup(folium.Html(html, script=True), max_width=500)
        folium.CircleMarker(
                    [d[1]["etrs89_lat"], d[1]["etrs89_long"]],
                    radius=6,
                    color=colors_flood[d[1]["floodability_index_ud"]],
                    fill=True,
                    fill_color=colors_flood[d[1]["floodability_index_ud"]],
                    fill_opacity=0.7,
                    popup=popup
            ).add_to(sligo_flood)

sligo_flood = add_legend(sligo_flood, 'Likelihood of flooding', colors = list(colors_flood.values()), labels = ('Very Low', 'Low', 'Moderate', 'Moderate to High', 'High', 'Very High'))
sligo_flood.save('/usr/local/spark/resources/data/visualization/Sligo_Flood_Rating.html')
print("FLOOD RATING MAP GENERATED AND SAVED")

#Plotting gamma_loan data - index search
print("\n------")
print("GENERATING GAMMA-LOAN INDEX SEARCH MAP")
print("------")

Sligo_gamma_loan=folium.Map(location=[lat,lang],zoom_start=10)
cluster=plugins.MarkerCluster().add_to(Sligo_gamma_loan)

for d in df_gamma_loan.iterrows(): 
        html = popup_html_index(d[0])
        popup = folium.Popup(folium.Html(html, script=True), max_width=600)
        folium.Marker(location=[d[1]["etrs89_lat"], d[1]["etrs89_long"]], popup=popup,name=d[1]["ecad_id"]).add_to(cluster)
Search(cluster,search_label='name',placeholder='Search for ECAD ID').add_to(Sligo_gamma_loan)            
Sligo_gamma_loan.save('/usr/local/spark/resources/data/visualization/Sligo_Gamma_Loan_IndexSearch.html')
print("GAMMA-LOAN INDEX SEARCH MAP GENERATED AND SAVED")