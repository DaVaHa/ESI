#! /bin/bash

# download source data
echo "########### Downloading source data.. ###########"
python3 getting_source_files.py

# processing source data
echo "########### Processing source data.. ###########"
python3 source_XBRU.py
python3 source_XAMS.py
python3 source_XPAR.py
python3 source_XLIS.py

python3 source_XPAR_PDF.py
python3 update_deduplicate.py XPAR_PDF
python3 corrections_XPAR_PDF.py
python3 deduplicate_XPAR_PDF.py
python3 update_XPAR_PDF.py 

# clean data
echo "########### Cleaning names.. ###########"
python3 cleaning_doubles.py

echo "########### Updating & deduplicating data.. ###########"
python3 update_deduplicate.py XBRU
python3 update_deduplicate.py XAMS
python3 update_deduplicate.py XPAR
python3 update_deduplicate.py XLIS

# add corrections
echo "########### Adding corrections.. ###########"
python3 corrections.py XBRU
python3 corrections.py XAMS
python3 corrections.py XPAR
python3 corrections.py XLIS

# calculate total_short_interest
echo "########### Calculating total short interest.. ###########"
python3 total_short_interest.py XBRU
python3 total_short_interest.py XAMS
python3 total_short_interest.py XPAR
python3 total_short_interest.py XLIS

# updating SummaryDB
echo "########### updating Issuers SummaryDB.db.. ###########"
python3 summary_issuers.py XBRU
python3 summary_issuers.py XAMS
python3 summary_issuers.py XPAR
python3 summary_issuers.py XLIS

# updating SummaryDB
echo "########### updating Notifications SummaryDB.db.. ###########"
python3 summary_notifications.py XBRU
python3 summary_notifications.py XAMS
python3 summary_notifications.py XPAR
python3 summary_notifications.py XLIS

# update Euronext data
echo "########### updating Euronext Euronext.db.. ###########"
python3 euronext_quandl.py
python3 euronext_corp_act.py

# graphing short interest
echo "########### Creating graphs.. ###########"
python3 graphs.py XBRU
python3 graphs.py XAMS
python3 graphs.py XPAR
python3 graphs.py XLIS

# graphing interactive bokeh graphs
echo "########### Creating bokeh graphs.. ###########"
python3 bokeh_graph.py XBRU
python3 bokeh_graph.py XAMS
python3 bokeh_graph.py XPAR
python3 bokeh_graph.py XLIS

# showing latest update by source
echo "########### Showing latest updates... ###########"
python3 check_latest_updates.py

# running web application
echo "########### Check web app ;) ###########"
python3 webapp.py




