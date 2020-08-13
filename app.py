import base64
import datetime
import io
import plotly.graph_objs as go
import dask
import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import plotly.express as px
import plotly.io as pio

px.set_mapbox_access_token('pk.eyJ1IjoiZXdpbGxpYW1zMjAyMCIsImEiOiJja2FpdTIxOXMwM2wzMnFtbmVmb3IzZDJ6In0.TVsQ-iu8bN4PQLkBCr6tQQ')


import pandas as pd

def ProcessRawDataAerisTxt(read_file,maxSpeed = '45',minSpeed = '0'):
    import pandas as pd
    from datetime import datetime
    import os
    from numpy import pi
    import numpy as np
    import csv
    from math import floor
    import swifter
    if (read_file.shape[0] != 0):
        xMaxCarSpeed = float(maxSpeed) / 2.23694  # CONVERTED TO M/S (default is 45mph)
        xMinCarSpeed = float(minSpeed) / 2.23694  # CONVERTED TO M/S (default is 2mph)
        #read_file = pd.read_csv (r'/Users/emilywilliams/Documents/GitHub/AMLD_Driving_Data/trussville_dat/RawData/Pico100073_200616_204907.txt')
        sHeader = ['TIMESTAMP','INLET','PRESS_MBAR','TEMPC','CH4','H20','C2H6','R','C2C1','BATTV','POWMV','CURRMA','SOCPER','LAT','LONG',
                     'U','V','W','TC','DIRDEG','VELOCITY','COMPASSDEG']
        read_file.columns = sHeader
        sOutHeader = ["DATE","TIME","SECONDS","NANOSECONDS","VELOCITY","U","V","W","BCH4","BRSSI",
                      "TCH4","TRSSI","PRESS_MBAR","INLET","TEMPC","CH4","H20","C2H6","R","C2C1",
                      "BATTV","POWMV","CURRMA","SOCPER","LAT","LONG"]
        read_file['fdate'] = read_file.apply(lambda x: datetime(int(x.TIMESTAMP[6:10]), int(x.TIMESTAMP[0:2]), int(x.TIMESTAMP[3:5]), int(x.TIMESTAMP[11:13]),
                         int(x.TIMESTAMP[14:16]), int(x.TIMESTAMP[17:19]), int(float(x.TIMESTAMP[19:23]) * 1000000)),axis = 1)
        read_file['seconds'] = read_file.swifter.apply(lambda x: x.fdate.strftime('%s.%f'),axis=1)

        read_file['DATE'] = read_file.fdate.swifter.apply(lambda x: x.strftime('%Y-%m-%d'))
        read_file['TIME'] = read_file.fdate.swifter.apply(lambda x: x.strftime('%H:%M:%S'))
        read_file['SECONDS'] = read_file.seconds.swifter.apply(lambda x: x[:10])
        read_file['NANOSECONDS'] = read_file.seconds.swifter.apply(lambda x: int(x[11:])*1000)
        read_file['BCH4'] = read_file.loc[:,'CH4']
        read_file['BRSSI'] = 0
        read_file['TRSSI'] = 0
        read_file['TCH4'] = read_file.loc[:,'CH4']

        wind_df = read_file.loc[read_file['LAT'].notnull(), ].reset_index(drop = True)
        wind_df5 = wind_df.loc[wind_df.VELOCITY > xMinCarSpeed, :]
        wind_df6 = wind_df.loc[wind_df5.VELOCITY < xMaxCarSpeed, :]
        del(wind_df,wind_df5)
        wind_df4 = wind_df6.copy().drop_duplicates()
        wind_df = wind_df4.loc[wind_df4.CH4.notnull(), :]
        return (wind_df)
def IdentifyPeaksAeris(xCar,proc_file,threshold = '.1',xTimeThreshold = '5.0',minElevated = '2',xB = '102',basePerc = '50'):
    import csv, numpy
    import geopandas as gpd
    import shutil
    import swifter
    try:
        baseCalc = float(basePerc)
        xABThreshold = float(threshold)
        minElevated = float(minElevated)
        xDistThreshold = 160.0                 # find the maximum CH4 reading of observations within street segments of this grouping distance in meters
        xSDF = 4                    # multiplier times standard deviation for floating baseline added to mean
        xB = int(xB)
        xTimeThreshold = float(xTimeThreshold)

        if (3 >1):
            fDate = 0; fTime = 1; fEpochTime = 2;
            fNanoSeconds = 3; fVelocity = 4;  fU = 5;
            fV = 6; fW = 7;fBCH4 = 8;
            fBRSSI = 9;
            fTCH4 = 10; TRSSI = 11;PRESS = 12;
            INLET = 13; TEMP = 14;CH4 = 15;
            H20 = 16;C2H6 = 17;R = 18;
            C2C1 = 19; BATT = 20;POWER = 21;
            CURR = 22;SOCPER = 23;fLat = 24;
            fLon = 25;

        #read data in from text file and extract desired fields into a list, padding with 5 minute and hourly average
            x1 = []; x2 = [];  x3 = [];
            x4 = []; x5 = []; x6 = [];x7 = []; x8 = []

            count = -1
            for index, row in proc_file.iterrows():
                    if count < 0:
                        count += 1
                        continue
                    datet = row['DATE'].replace("-", "") + row['DATE'].replace(":", "")
                    ## if not engineering
                    epoch = float(row['seconds'])
                    datetime = row['DATE'].replace("-", "") + row['TIME'].replace(":", "")
                    x1.append(row['seconds']);
                    x2.append(datetime);
                    if row['LAT'] == '':
                        x3.append('')
                    elif row['LAT'] != '':
                        x3.append(float(row['LAT']));
                    if row['LONG'] == '':
                        x4.append('')
                    elif row['LONG'] != '':
                        x4.append(float(row['LONG']));

                    x5.append(float(row['CH4']));
                    x6.append(float(row['CH4']))
                    x7.append(0.0);
                    x8.append(0.0)
                    count += 1
            print("Number of observations processed: " + str(count))

        #convert lists to numpy arrays
        aEpochTime = numpy.array(x1); aDateTime = numpy.array(x2);
        aLat = numpy.array(x3);
        aLon = numpy.array(x4); aCH4 = numpy.array(x5); aTCH4 = numpy.array(x6)
        aMean = numpy.array(x7); aThreshold = numpy.array(x8)

        xLatMean = numpy.mean(aLat)
        xLonMean = numpy.mean(aLon)
        lstCH4_AB = []

        #generate list of the index for observations that were above the threshold
        for i in range(0,count-2):
            if ((count-2)>xB):
                topBound = min((i+xB), (count-2))
                botBound = max((i-xB), 0)

                for t in range(min((i+xB), (count-2)), i, -1):
                    if float(aEpochTime[t]) < (float(aEpochTime[i])+(xB/2)):
                        topBound = t
                        break
                for b in range(max((i-xB), 0), i):
                    if float(aEpochTime[b]) > (float(aEpochTime[i])-(xB/2)):
                        botBound = b
                        break

                xCH4Mean = numpy.percentile(aCH4[botBound:topBound],baseCalc)
               # xCH4SD = numpy.std(aCH4[botBound:topBound])
            else:
                xCH4Mean = numpy.percentile(aCH4[0:(count-2)],baseCalc)
                #xCH4SD = numpy.std(aCH4[0:(count-2)])
            xThreshold = xCH4Mean + (xCH4Mean * xABThreshold)

            if (aCH4[i] > xThreshold):
                lstCH4_AB.append(i)
                aMean[i] = xCH4Mean    #insert mean + SD as upper quartile CH4 value into the array to later retreive into the peak calculation
                aThreshold[i] = xThreshold

        # now group the above baseline threshold observations into groups based on distance threshold
        lstCH4_ABP = []
        xDistPeak = 0.0
        xCH4Peak = 0.0
        xTime = 0.0
        cntPeak = 0
        cnt = 0
        sID = ""
        sPeriod5Min = ""
        prevIndex = 0
        for i in lstCH4_AB:
            if (cnt == 0):
                xLon1 = aLon[i]; xLat1 = aLat[i]
            else:
                # calculate distance between points
                xDist = haversine(xLat1, xLon1, aLat[i], aLon[i])
                xDistPeak += xDist
                xCH4Peak += (xDist * (aCH4[i] - aMean[i]))
                xLon1 = aLon[i]; xLat1 = aLat[i]
                if (sID == ""):
                    xTime = float(aEpochTime[i])
                    sID = str(xCar) + "_" + str(xTime)
                    sPeriod5Min = str(int((float(aEpochTime[i]) - 1350000000) / (30 * 1))) # 30 sec
                if ((float(aEpochTime[i])-float(aEpochTime[prevIndex])) > xTimeThreshold):       #initial start of a observed peak
                    cntPeak += 1
                    xTime = float(aEpochTime[i])
                    xDistPeak = 0.0
                    xCH4Peak = 0.0
                    sID = str(xCar) + "_" + str(xTime)
                    sPeriod5Min = str(int((float(aEpochTime[i]) - 1350000000) / (30 * 1))) # 30 sec
                    #print str(i) +", " + str(xDist) + "," + str(cntPeak) +"," + str(xDistPeak)
                lstCH4_ABP.append([sID, xTime, aEpochTime[i], aDateTime[i], aCH4[i], aLon[i], aLat[i], aMean[i] ,aThreshold[i], xDistPeak, xCH4Peak, aTCH4[i], sPeriod5Min])
            cnt += 1
            prevIndex = i

        # Finding peak_id larger than 160.0 m
        tmpsidlist = []
        for r in lstCH4_ABP:
            if (float(r[9])>160.0) and (r[0] not in tmpsidlist):
                tmpsidlist.append(r[0])
        cntPeak-=len(tmpsidlist)
        print ( "Number of peaks found: " + str(cntPeak) + "\n")
        s = ["OP_NUM","OP_EPOCHSTART","OB_EPOCH","OB_DATETIME",
             "OB_CH4","OB_LON","OB_LAT","OB_CH4_BASELINE","OB_CH4_THRESHOLD",
             "OP_PEAK_DIST_M","OP_PEAK_CH4","OB_TCH4","OB_PERIOD5MIN"]

        truecount = 0
        openFile = pd.DataFrame(lstCH4_ABP, columns=s)
        from shapely.geometry import Point
        if openFile.shape[0] != 0:
            tempCount = openFile.groupby('OP_NUM',as_index=False).OP_EPOCHSTART.count().rename(columns={'OP_EPOCHSTART':'Frequency'})
            tempCount = tempCount.loc[tempCount.Frequency>=minElevated,:]
            if tempCount.shape[0]==0:
             print("No Observed Peaks with enough Elevated Readings Found in the file: " + str(xFilename) )
            elif tempCount.shape[0]!=0:
                oFile = pd.merge(openFile,tempCount,on=['OP_NUM'])
                openFile = oFile.copy()
                del(oFile)
                openFile['minElevated'] = openFile.apply(lambda x: int(minElevated),axis=1)
                openFile['OB_CH4_AB'] = openFile.loc[:,'OB_CH4'].sub(openFile.loc[:,'OB_CH4_BASELINE'], axis = 0)
                locIdentified = weightedLoc(openFile, 'OB_LAT', 'OB_LON', 'OP_NUM', 'OB_CH4_AB').loc[:, :].rename(
                    columns={'OB_LAT': 'pk_LAT', 'OB_LON': 'pk_LON'}).reset_index(drop=True)
                final = locIdentified.merge(openFile, on=['OP_NUM']).loc[:,
                        ['OP_NUM', 'pk_LON', 'pk_LAT', 'OB_DATETIME',
                         ]]
                final['DATE'] = final.OB_DATETIME.swifter.apply(
                    lambda x: str(x[:4]) + str('-') + str(x[4:6]) + str('-') + str(x[6:8]))
                final = final.drop(columns=['OB_DATETIME']).drop_duplicates().reset_index(drop=True)

                return(final)
        elif openFile.shape[0] == 0:
            print("No Observed Peaks Found in the file: ")
    except ValueError:
            print ("Error in Identify Peaks")
            return False
def haversine(lat1, lon1, lat2, lon2, radius=6371): # 6372.8 = earth radius in kilometers
    from math import radians, sin, cos, sqrt, asin

    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    c = 2*asin(sqrt(sin(dLat/2)**2 + cos(lat1)*cos(lat2)*sin(dLon/2)**2))

    return radius*c*1000 #return in meters
def weightedLoc(df,lat,lon,by,val2avg):
    import pandas as pd
    df_use = df.loc[:,[(lat),(lon),(by),val2avg]]
    df_use.loc[:,'lat_wt'] = df_use.apply(lambda y: y[lat] * y[val2avg],axis = 1).copy()
    df_use.loc[:,'lon_wt'] = df_use.apply(lambda y: y[lon] * y[val2avg],axis = 1).copy()


    #sumwts =pd.DataFrame( df_use.groupby('min_read').apply(lambda y: sumthing(y['pk_maxCH4_AB'])),columns = {'totwts'})
    sumwts = pd.DataFrame(df_use.copy().groupby(str(by)).apply(lambda y: sumthing(y[str(val2avg)])),columns = {'totwts'})
    sumwts.loc[:,'min_reads'] = sumwts.copy().index
    sumwts = sumwts.reset_index(drop=True).rename(columns={"min_reads": str(by)})

    #sumwts = sumwts.rename(columns={"min_reads": str(by)})

    totlats = pd.DataFrame(df_use.groupby(str(by)).apply(lambda y: sumthing(y['lat_wt'])),columns = ['totlats'])

    totlats['min_reads'] = totlats.index.copy()
    totlats = totlats.reset_index(drop=True)
    totlats = totlats.rename(columns={"min_reads": str(by)})

    totlons = pd.DataFrame(df_use.groupby(str(by)).apply(lambda y: sumthing(y['lon_wt'])),columns = ['totlons'])

    totlons['min_reads'] = totlons.index.copy()
    totlons = totlons.reset_index(drop=True)
    totlons = totlons.rename(columns={"min_reads": str(by)})

    df_use = pd.merge(totlats,df_use,on = str(by))
    df_use = pd.merge(totlons,df_use,on = str(by))
    df_use = pd.merge(sumwts,df_use,on = str(by))


    df_use.loc[:,'overall_LON'] = df_use.apply(lambda y: y['totlons']/y['totwts'],axis = 1)
    df_use.loc[:,'overall_LAT'] = df_use.apply(lambda y: y['totlats']/y['totwts'],axis = 1)

    toreturn = df_use.loc[:,[(str(by)),('overall_LON'),('overall_LAT')]].drop_duplicates()
    toreturn = toreturn.rename(columns = {'overall_LON':str(lon),'overall_LAT':str(lat)})

    return(toreturn)

def sumthing(thing):
    return(sum(thing))


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

colors = {
    "graphBackground": "#F5F5F5",
    "background": "#ffffff",
    "text": "#000000"
    }

app.layout = html.Div([
dcc.Upload(
    id='upload-data',
    children=html.Div([
        '',
        html.A('Select .txt File to Analyse')
    ]),
    style={
        'width': '100%',
        'height': '60px',
        'lineHeight': '60px',
        'borderWidth': '1px',
        'borderStyle': 'dashed',
        'borderRadius': '5px',
        'textAlign': 'center',
        'margin': '10px'
    },
    # Allow multiple files to be uploaded
    multiple=True
),
    html.Div(id = "UploadConf"),
#dcc.Graph(id='leakGraph'),
html.Div(id='output-data-upload')
])


@app.callback(Output('Mygrapha', 'figure'), [
Input('upload-data', 'contents'),
Input('upload-data', 'filename')
])
def update_graph(contents, filename):
    fig = {
        'layout': go.Layout(
            plot_bgcolor=colors["graphBackground"],
            paper_bgcolor=colors["graphBackground"]
        )
    }

    if contents:
        contents = contents[0]
        filename = filename[0]
        df = parse_data(contents, filename)
        df = df.set_index(df.columns[0])
        fig['data'] = df.iplot(
            asFigure=True, kind='scatter', mode='lines+markers', size=1)

    return fig


def parse_data(contents, filename):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            # Assume that the user uploaded a CSV or TXT file
            df = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))
        elif 'txt' or 'tsv' in filename:
            # Assume that the user upl, delimiter = r'\s+'oaded an excel file
            df = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))#, delimiter=r'\s+')

    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])

    return df

@app.callback(Output('UploadConf', 'children'),
          [
Input('upload-data', 'contents'),
Input('upload-data', 'filename')
])
def updateWords(contents, filename):
    if contents:
        return("Yay, uploaded File")
    elif not contents:
        return("No file uploaded")


@app.callback(Output('output-data-upload', 'children'),
          [
Input('upload-data', 'contents'),
Input('upload-data', 'filename')
])
def update_table(contents, filename):
    table = html.Div()

    if contents:
        contents = contents[0]
        filename = filename[0]
        df2 = parse_data(contents, filename)
        proc_file = ProcessRawDataAerisTxt(df2, '45', '0')
        df3 = IdentifyPeaksAeris('truss', proc_file, '.1', '5.0', '2', '102', '50')
        df3.columns = ['Peak Name','LONGITUDE','LATITUDE','DATE']
        df3['ID']= df3.index
        df = df3.loc[:,['ID','DATE','LONGITUDE','LATITUDE','Peak Name']]
        if type(df) == 'NoneType': #df == "None":
            table = html.Div([])
        elif type(df) != 'NoneType': #df != "None":
            table = html.Div([
                html.H5(str('Leaks found in: ') + str(filename)),
                dash_table.DataTable(
                    data=df.to_dict('rows'),
                    columns=[{'name': i, 'id': i} for i in df.columns]
                ),
                html.Hr(),
                html.Div('Raw Content'),
                html.Pre(contents[0:200] + '...', style={
                    'whiteSpace': 'pre-wrap',
                    'wordBreak': 'break-all'
                })
            ])

    return table


@app.callback(Output('leakGrapha', 'figure'),
          [
Input('upload-data', 'contents'),
Input('upload-data', 'filename')
])
def updateGraph(contents, filename):
    if contents:
        contents = contents[0]
        filename = filename[0]
        df2 = parse_data(contents, filename)
        proc_file = ProcessRawDataAerisTxt(df2, '45', '0')
        df3 = IdentifyPeaksAeris('truss', proc_file, '.1', '5.0', '2', '102', '50')
        df3.columns = ['Peak Name', 'LONGITUDE', 'LATITUDE', 'DATE']
        df3['ID'] = df3.index
        usedat = df3.loc[:, ['ID', 'DATE', 'LONGITUDE', 'LATITUDE', 'Peak Name']]
        if usedat.shape[0] != 0:
           # fig = px.scatter_mapbox(usedat, lat="LATITUDE", lon="LONGITUDE",
          #                           color='ID', size_max=15, zoom=11,
          #                           hover_data={'DATE'})
            #fig.add_trace(fig2.data[0])
            fig = px.scatter_mapbox(usedat, lat="LATITUDE", lon="LONGITUDE", color="ID",
                                    color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=10)
            fig = px.scatter_mapbox(usedat, lat="LATITUDE", lon="LONGITUDE",
                                    size_max=200, zoom=11)
    elif not contents:
        fig = px.scatter_mapbox()

    return (fig)



if __name__ == '__main__':
    app.run_server(debug=False)
