'''
This script creates an interactive bokeh graph.
'''
from bokeh.io import output_file, show
from bokeh.plotting import figure
from bokeh.palettes import Spectral6
from bokeh.models import LinearAxis, Range1d, HoverTool, ColumnDataSource
from bokeh.resources import CDN
from bokeh.embed import file_html

import sqlite3 as lite
import pandas as pd
import time
from functions import Logging
import sys

# measure duration of run
startTime = time.time()

# parameters
try:
    mic = sys.argv[1].upper()
except:
    Logging("\nNo argument given, please provide MIC for creating the bokeh graphs.\n")
    exit()
##mic = 'XBRU'


# create function to get all issuers
def GetIssuersMIC(mic):
    
    # connections
    db = "SummaryDB.db"
    con = lite.connect(db)
    cur = con.cursor()

    # get all issuers
    cur.execute('''SELECT DISTINCT ISSUER, PRETTY_NAME, GRAPH_NAME, QUANDL_CODE
                   FROM Issuers WHERE MIC="{}" and DELETED=0 ORDER BY ISSUER;'''.format(mic))
    issuers = [(i[0],i[1],i[2],i[3]) for i in cur.fetchall()]

    # closing connections
    cur.close()
    con.close()
    
    return issuers



def BokehGraph(issuer_tuple, mic):
    
    issuer = issuer_tuple[0]   #issuer
    title_name = issuer_tuple[1]  #pretty_name
    graph_name = issuer_tuple[2]    #graph_name
    if issuer_tuple[3] == None:
        return None
    quandl_code = issuer_tuple[3].replace('EURONEXT/', '')  #quandl_code

    print(issuer)
    # connections
    db = "{}_ShortInterest.db".format(mic)
    con = lite.connect(db)
    cur = con.cursor()
    conn = lite.connect('Euronext.db')
    curr = conn.cursor()

    # get data  
    issuer_tbl = issuer.strip().lower().replace(' ', '_').replace('-', '_').replace('.','').replace("'","") #total_short_interest.py
    cur.execute('SELECT DATE, TOTAL FROM "{}";'.format(issuer_tbl))
    data = cur.fetchall()

    # short interest
    df = pd.DataFrame(data)
    df.columns = ['Date', 'Interest']
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    
    # get Euronext prices
    curr.execute('SELECT Date, "{}" from Prices_incl_CA;'.format(quandl_code))
    dff = pd.DataFrame(curr.fetchall())
    dff.columns = ['Date','Price']
    dff['Date'] = pd.to_datetime(dff['Date'])
    dff.set_index('Date', inplace=True)
    
    # combine dataframes
    combi = pd.merge(df, dff, how='left', left_index=True, right_index=True)
    combi['Price'] = combi['Price'].fillna(method='ffill')

    #print(combi.head())
    #print(combi.tail())
    
    # bokeh graph
    # y axes min & max
    min_y1 = -0.2
    max_si = combi['Interest'].max()
    if max_si > 15: max_y1 = 20.6
    elif max_si > 10 : max_y1 = 15.6
    elif max_si > 5 : max_y1 = 10.6
    else: max_y1 = 5.3
    
    min_y2 = combi['Price'].min()/1.4
    max_y2 = combi['Price'].max()*1.1

    # basic formatting
    plot = figure(tools=['pan', 'box_zoom','wheel_zoom', 'reset','undo', 'redo'],toolbar_location="above", plot_width=750, plot_height=500, x_axis_type='datetime', y_range=(min_y1, max_y1), title=title_name)
    plot.xaxis.axis_label='Date'  # x-axis
    plot.yaxis.axis_label='Short Interest (%)'

    # source data
    source=ColumnDataSource(data={'date':combi.index, 'si':combi['Interest'], 'price':combi['Price'], 'dateStr': pd.Series(combi.index.format())})
    
    # plotting price
    plot.extra_y_ranges = {"price": Range1d(start=min_y2, end=max_y2)}
    plot.add_layout(LinearAxis(y_range_name="price", axis_label='Stock Price'), 'right')
    plot.line(x='date', y='price', source=source, color=Spectral6[0], y_range_name="price", legend='Stock Price (right)', line_width=2)  # plotting price

    # plotting short interest
    plot.line(x='date', y='si', source=source, color=Spectral6[4], legend='Short Interest (left)', line_width=3) # plotting SI
    
    # add hovertool
    hover = HoverTool(tooltips=[('Date', '@dateStr'), ('Short Interest', '@si{0.00}'), ('Price', '@price{0.00}')] )
    plot.add_tools(hover)
    
    # end formatting
    plot.legend.location = 'top_left'
    plot.background_fill_color='lightgray'

    #output file
    #output_file("templates/bokeh_graph.html")
    #show(plot)

    # to html file
    html = file_html(plot, CDN)
    bokeh_name = graph_name.replace('.png', '')
    
    with open("static/bokeh/{}.html".format(bokeh_name), 'w') as h:
        h.write(html)

        

# only run when this is main script
if __name__ == "__main__":
    
    Logging("Creating Bokeh Graphs for MIC: {}\n".format(mic))
    
    # get issuers for MIC
    issuers = GetIssuersMIC(mic)
    
    # run function for every issuer
    for iss in issuers:
        try:
            BokehGraph(iss, mic)
        except Exception as e:
            print("Error with {}".format(iss))
            print("Error: {}".format(str(e)))

    # print duration of run
    endTime = time.time()
    Logging("\nScript ran for {} seconds.".format(round(endTime-startTime,2)))



    
