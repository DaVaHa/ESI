'''
This script will scrape & prepare the source files for processing.
'''

import pyautogui
import time
import webbrowser
import os
from functions import TimeStamp, Logging
import pandas as pd
import xlsxwriter


pyautogui.PAUSE = 1
pyautogui.FAILSAFE = True


##################  XBRU  ##################
def SourceXBRU():
    
    # open url and wait few seconds
    link_xbru = "https://www.fsma.be/nl/file/disclosure-net-short-positions-fsmaxlsx"
    webbrowser.open(link_xbru)
    time.sleep(10)
  
    # click on download
    pyautogui.hotkey('ctrl', 'winleft','up')
    pyautogui.moveTo(2000,630, duration=0.5)
    pyautogui.click()
    time.sleep(5)

    # move and rename downloads
    files = []
    for f in os.listdir('/home/daniel/Downloads/'):
        if ('disclosure' in f or 'fsma' in f) and f.endswith('.xlsx'):
            files.append(f)
            
    i=0
    try:
        oldFileName = '/home/daniel/Downloads/{}'.format(files[0])
    except Exception as e:
        Logging("Exception: {}".format(str(e)))
        if i < 4:
            i+=1
            SourceXBRU()
        else:
            return
    
    newFileName = '/home/daniel/Desktop/Scripts/_ShortNotifications/XBRU_{}.xlsx'.format(TimeStamp('%Y%m%d_%Hh%M'))
    os.rename(oldFileName, newFileName)

    # Done.   
    Logging('File for XBRU ready.')



##################  XAMS  ##################
def SourceXAMS():

    # open urls and download
    link_actueel = "https://www.afm.nl/nl-nl/professionals/registers/meldingenregisters/netto-shortposities-actueel"
    webbrowser.open(link_actueel)
    time.sleep(10)
    pyautogui.hotkey('ctrl', 'winleft','up')
    pyautogui.moveTo(2735, 475, duration=0.5)
    pyautogui.click()
    time.sleep(5)
    
    link_historie = "https://www.afm.nl/nl-nl/professionals/registers/meldingenregisters/netto-shortposities-historie"
    webbrowser.open(link_historie)
    time.sleep(10)
    pyautogui.moveTo(2735, 475, duration=0.5)
    pyautogui.click()
    time.sleep(5)

    # find csv files
    csv_act = []
    csv_hist = []
    for f in os.listdir('/home/daniel/Downloads/'):
        if 'short' in f and 'actu' in f and f.endswith('.csv'):
            csv_act.append(f)
        if 'short' in f and 'hist' in f and f.endswith('.csv'):
            csv_hist.append(f)

    i=0
    try:    
        act_file = '/home/daniel/Downloads/{}'.format(csv_act[0])
        hist_file = '/home/daniel/Downloads/{}'.format(csv_hist[0])
        #print(csv_act)
        #print(csv_hist)
    except Exception as e:
        Logging("Exception: {}".format(str(e)))
        if i < 4:
            i+=1
            SourceXAMS()
        else:
            return
    
    # create excel file of csv files
    xlsx_name = "/home/daniel/Desktop/Scripts/_ShortNotifications/XAMS_{}.xlsx".format(TimeStamp('%Y%m%d_%Hh%M'))
    writer = pd.ExcelWriter(xlsx_name, engine='xlsxwriter')
    
    df_act = pd.read_csv(act_file, delimiter=';', encoding='latin-1')
    df_act.to_excel(writer, sheet_name='Actueel')

    df_hist = pd.read_csv(hist_file, delimiter=';', encoding='latin-1')
    df_hist.to_excel(writer, sheet_name='Historie')

    writer.save()

    # move csv files to Files
    newActFileName = '/home/daniel/Desktop/Scripts/_ShortNotifications/Files/{}_{}.csv'.format(csv_act[0][:-4],TimeStamp('%Y%m%d_%Hh%M'))
    newHistFileName = '/home/daniel/Desktop/Scripts/_ShortNotifications/Files/{}_{}.csv'.format(csv_hist[0][:-4],TimeStamp('%Y%m%d_%Hh%M'))
    os.rename(act_file, newActFileName)
    os.rename(hist_file, newHistFileName)

    
    # Done.   
    Logging('File for XAMS ready.')




################## XPAR  ##################

def SourceXPAR():
    
    # open url and wait few seconds
    link_xpar = "http://www.amf-france.org/en_US/Acteurs-et-produits/Marches-financiers-et-infrastructures/Ventes-a-decouvert/Consolidation-des-publications.html"
    webbrowser.open(link_xpar)
    time.sleep(10)
  
    # click on download
    pyautogui.hotkey('ctrl', 'winleft','up')
    pyautogui.moveTo(2560,532, duration=0.5)
    pyautogui.click()
    time.sleep(5)

    # move and rename downloads
    files = []
    for f in os.listdir('/home/daniel/Downloads/'):
        if ('fichier' in f or 'VAD' in f) and '.xls' in f:
            files.append(f)
    
    i=0
    try:
        oldFileName = '/home/daniel/Downloads/{}'.format(files[0])
    except Exception as e:
        Logging("Exception: {}".format(str(e)))
        if i < 4:
            i+=1
            SourceXPAR()
        else:
            return
    
    newFileName = '/home/daniel/Desktop/Scripts/_ShortNotifications/XPAR_{}.xlsx'.format(TimeStamp('%Y%m%d_%Hh%M'))
    os.rename(oldFileName, newFileName)

    # Done.   
    Logging('File for XPAR ready.')





if __name__ == '__main__':

    # run functions
    try:
        SourceXBRU()
    except Exception as e:
        print("Something went wrong with the source file : XBRU")
        print("Exception: {}".format(str(e)))

    try:
        SourceXAMS()
    except Exception as e:
        print("Something went wrong with the source file : XAMS")
        print("Exception: {}".format(str(e)))    

    try:
        SourceXPAR()
    except Exception as e:
        print("Something went wrong with the source file : XPAR")
        print("Exception: {}".format(str(e)))    


    
