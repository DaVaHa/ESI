'''
This sets the pretty names in SummaryDB.db.
'''
import sqlite3 as lite

print("\nPrettifying names..\n")

prettify = [
    ("AALBERTS INDUSTRIES N.V.", "Aalberts Industries"),
    ("ACCELL GROUP N.V.", "Accell Group"),
    ("AEGON N.V.", "Aegon"),
    ("AKZONOBEL N.V.", "AkzoNobel"),
    ("ALTICE N.V.", "Altice"),
    ("AMG ADVANCED METALLURGICAL GROUP N.V.", "Advanced Metallurgical Group (AMG)"),
    ("APERAM", "Aperam"),
    ("ARCADIS N.V.", "Arcadis"),
    ("ARCELORMITTAL S.A.", "ArcelorMittal"),
    ("ASM INTERNATIONAL N.V.", "ASM International"),
    ("ASML HOLDING N.V.", "ASML Holding"),
    ("BASIC-FIT N.V.", "Basic-Fit"),
    ("BE SEMICONDUCTOR INDUSTRIES N.V.", "BE Semiconductor Industries"),
    ("BINCKBANK N.V.", "BinckBank"),
    ("BRUNEL INTERNATIONAL N.V.", "Brunel International"),
    ("CORBION N.V.", "Corbion"),
    ("CORE LABORATORIES N.V.", "Core Laboratories"),
    ("CORIO N.V.", "Corio"),
    ("DELTA LLOYD N.V.", "Delta Lloyd"),
    ("DOCDATA N.V.", "Docdata"),
    ("EUROCOMMERCIAL PROPERTIES N.V.", "Eurocommercial Properties"),
    ("EXACT HOLDING N.V.", "Exact Holding"),
    ("FLOW TRADERS N.V.", "Flow Traders"),
    ("FUGRO N.V.", "Fugro"),
    ("GALAPAGOS N.V.", "Galapagos"),
    ("GEMALTO N.V.", "Gemalto"),
    ("HEIJMANS N.V.", "Heijmans"),
    ("KENDRION N.V.", "Kendrion"),
    ("KONINKLIJKE AHOLD DELHAIZE N.V.", "Ahold Delhaize"),
    ("KONINKLIJKE BAM GROEP N.V.", "BAM Groep"),
    ("KONINKLIJKE BOSKALIS WESTMINSTER N.V.", "Boskalis Westminster"),
    ("KONINKLIJKE DSM N.V.", "DSM"),
    ("KONINKLIJKE KPN N.V.", "KPN"),
    ("KONINKLIJKE TEN CATE N.V.", "Ten Cate"),
    ("KONINKLIJKE VOPAK N.V.", "Vopak"),
    ("KONINKLIJKE WESSANEN N.V.", "Wessanen"),
    ("NSI N.V.", "NSI"),
    ("NUTRECO N.V.", "Nutreco"),
    ("OCI N.V.", "OCI"),
    ("ORDINA N.V.", "Ordina"),
    ("PHARMING GROUP N.V.", "Pharming Group"),
    ("PHELIX N.V.", "Phelix"),
    ("PHILIPS LIGHTING N.V.", "Philips Lighting"),
    ("POSTNL N.V.", "PostNL"),
    ("ROYAL DUTCH SHELL PLC", "Royal Dutch Shell"),
    ("ROYAL IMTECH N.V.", "Royal Imtech"),
    ("SBM OFFSHORE N.V.", "SBM Offshore"),
    ("SNS REAAL N.V.", "SNS Reaal"),
    ("TKH GROUP N.V.", "TKH Group"),
    ("TNT EXPRESS N.V.", "TNT Express"),
    ("TOMTOM N.V.", "TomTom"),
    ("UNIBAIL-RODAMCO", "Unibail-Rodamco"),
    ("USG PEOPLE N.V.", "USG People"),
    ("VASTNED RETAIL N.V.", "Vastned Retail"),
    ("WERELDHAVE N.V.", "Wereldhave"),
    ("WOLTERS KLUWER N.V.", "Wolters Kluwer"),
    ("ZIGGO N.V.", "Ziggo"),
    ("AB SCIENCE", "AB Science"),
    ("ACCOR", "Accor"),
    ("ADOCIA", "Adocia"),
    ("AFFINE R.E.", "Affine"),
    ("AIR FRANCE-KLM", "Air France-KLM"),
    ("AIRBUS GROUP SE", "Airbus Group"),
    ("ALCATEL LUCENT", "Alcatel-Lucent"),
    ("ALSTOM", "Alstom"),
    ("ALTEN", "Alten"),
    ("ALTRAN TECHNOLOGIES", "Altran Technologies"),
    ("ANF IMMOBILIER", "ANF Immobilier"),
    ("ARKEMA", "Arkema"),
    ("AROUNDTOWN PROPERTY HOLDINGS", "Aroundtown Property Holdings"),
    ("ARTPRICE.COM", "Artprice.com"),
    ("ASSYSTEM", "Assystem"),
    ("ATOS SE", "Atos"),
    ("AUSY", "Ausy"),
    ("AVANQUEST", "Avanquest"),
    ("BELVEDERE", "Belvédère"),
    ("BENETEAU", "Beneteau"),
    ("BIOMERIEUX", "bioMérieux"),
    ("BOLLORE", "Bolloré"),
    ("BOURBON", "Bourbon"),
    ("BOUYGUES", "Bouygues"),
    ("BUREAU VERITAS", "Bureau Veritas"),
    ("BUREAU VERITAS REGISTRE INTERNATIONAL DE CLASSIFICATION DE NAVIRES ET D'AERONEFS", "Bureau Veritas"),
    ("CAP GEMINI", "Capgemini"),
    ("CARREFOUR", "Carrefour"),
    ("CASINO GUICHARD-PERRACHON", "Casino Guichard-Perrachon"),
    ("CELLECTIS", "Cellectis"),
    ("CGG", "CGG"),
    ("CHARGEURS", "Chargeurs"),
    ("COMPAGNIE DE SAINT-GOBAIN", "Saint-Gobain"),
    ("COMPAGNIE GENERALE DE GEOPHYSIQUE - VERITAS", "CGG - Veritas"),
    ("COMPAGNIE GENERALE DES ETABLISSEMENTS MICHELIN", "Michelin"),
    ("COMPAGNIE INDUSTRIELLE ET FINANCIERE D’INGENIERIE INGENICO", "Ingenico"),
    ("COMPAGNIE PLASTIC OMNIUM", "Plastic Omnium"),
    ("DANONE", "Danone"),
    ("DASSAULT SYSTEMES", "Dassault Systèmes"),
    ("DBV TECHNOLOGIES", "DBV Technologies"),
    ("DEVOTEAM", "Devoteam"),
    ("EDENRED", "Edenred"),
    ("EIFFAGE", "Eiffage"),
    ("ELECTRICITE DE FRANCE", "Electricité de France (EDF)"),
    ("ELIOR GROUP", "Elior group"),
    ("ELIS", "Elis"),
    ("ERYTECH PHARMA", "Erytech Pharma"),
    ("ESSILOR INTERNATIONAL", "Essilor International"),
    ("ETABLISSEMENTS MAUREL ET PROM", "Maurel & Prom"),
    ("EUROFINS SCIENTIFIC SE", "Eurofins Scientific"),
    ("EURONEXT NV", "Euronext"),
    ("EUROPCAR GROUPE", "Europcar Groupe"),
    ("EUTELSAT COMMUNICATIONS", "Eutelsat Communications"),
    ("FAURECIA", "Faurecia"),
    ("FONCIERE DES REGIONS", "Foncière des Régions"),
    ("GAMELOFT", "Gameloft"),
    ("GAMELOFT SE", "Gameloft"),
    ("GAZTRANSPORT ET TECHNIGAZ", "Gaztransport & Technigaz"),
    ("GECINA", "Gecina"),
    ("GEMALTO NV", "Gemalto"),
    ("GENFIT", "Genfit"),
    ("GROUPE EUROTUNNEL SE", "Groupe Eurotunnel"),
    ("GROUPE FNAC", "Groupe Fnac"),
    ("GROUPE STERIA SCA", "Groupe Steria"),
    ("GUERBET", "Guerbet"),
    ("HAVAS", "Havas"),
    ("HI-MEDIA", "Hi-Media"),
    ("HIGHCO", "HighCo"),
    ("ICADE", "Icade"),
    ("ILIAD", "Iliad"),
    ("INGENICO", "Ingenico"),
    ("INNATE PHARMA", "Innate Pharma"),
    ("JCDECAUX SA", "JCDecaux"),
    ("KLEPIERRE", "Klepierre"),
    ("L'AIR LIQUIDE", "Air Liquide"),
    ("LAFARGE", "Lafarge"),
    ("LAFARGEHOLCIM LTD", "LafargeHolcim"),
    ("LAGARDERE SCA", "Lagardère"),
    ("MARIE BRIZARD WINE AND SPIRITS", "Marie Brizard Wine & Spirits"),
    ("MERCIALYS", "Mercialys"),
    ("METABOLIC EXPLORER", "METabolic EXplorer"),
    ("METROPOLE TELEVISION", "Groupe M6"),
    ("MONTUPET S.A.", "Montupet"),
    ("NATUREX", "Naturex"),
    ("NEOPOST S.A.", "Neopost"),
    ("NEXANS", "Nexans"),
    ("NEXITY", "Nexity"),
    ("NICOX SA", "NicOx"),
    ("ORANGE", "Orange"),
    ("ORPEA", "Orpea"),
    ("PAGESJAUNES GROUPE", "PagesJaunes"),
    ("PARROT", "Parrot"),
    ("PERNOD RICARD", "Pernod Ricard"),
    ("PEUGEOT S.A.", "Peugeot"),
    ("PIERRE ET VACANCES", "Pierre & Vacances"),
    ("POXEL", "Poxel"),
    ("PRIMECITY INVESTMENT PLC", "Primecity Investment"),
    ("PUBLICIS GROUPE SA", "Publicis Groupe"),
    ("RALLYE", "Rallye"),
    ("REMY COINTREAU", "Remy Cointreau"),
    ("RENAULT", "Renault"),
    ("REXEL", "Rexel"),
    ("RUBIS", "Rubis"),
    ("SA LE NOBLE AGE", "Le Noble Age"),
    ("SAFRAN", "Safran"),
    ("SAFT GROUPE S.A.", "Saft Groupe"),
    ("SCOR SE", "SCOR"),
    ("SEQUANA", "Sequana"),
    ("SES", "SES"),
    ("SFR GROUP", "SFR Group"),
    ("SOITEC", "Soitec"),
    ("SOLOCAL GROUP", "Solocal Group"),
    ("SOPRA GROUP", "Sopra Group"),
    ("SOPRA STERIA GROUP", "Sopra Steria Group"),
    ("SPIE SA", "SPIE"),
    ("SRP GROUPE", "SRP Groupe"),
    ("STMICROELECTRONICS NV", "STMicroelectronics"),
    ("SUEZ", "Suez"),
    ("SUEZ ENVIRONNEMENT COMPANY", "Suez Environnement"),
    ("TECHNICOLOR", "Technicolor"),
    ("TECHNIP", "Technip"),
    ("TECHNIPFMC PLC", "TechnipFMC"),
    ("TELEVISION FRANCAISE 1", "Télévision Française 1"),
    ("THALES", "Thales"),
    ("THEOLIA", "Theolia"),
    ("UBISOFT ENTERTAINMENT", "Ubisoft Entertainment"),
    ("VALEO", "Valeo"),
    ("VALLOUREC", "Vallourec"),
    ("VEOLIA ENVIRONNEMENT", "Veolia Environnement"),
    ("VIRBAC", "Virbac"),
    ("WENDEL", "Wendel"),
    ("ZODIAC AEROSPACE", "Zodiac Aerospace"),
    ("ABLYNX", "Ablynx"),
    ("AEDIFICA", "Aedifica"),
    ("AGFA GEVAERT NV", "Agfa-Gevaert"),
    ("BARCO", "Barco"),
    ("BEFIMMO", "Befimmo"),
    ("BEKAERT", "Bekaert"),
    ("BPOST", "Bpost"),
    ("CELYAD", "Celyad"),
    ("COFINIMMO", "Cofinimmo"),
    ("COLRUYT", "Colruyt"),
    ("DELHAIZE GROUP", "Delhaize Group"),
    ("DEXIA SA", "Dexia"),
    ("ECONOCOM", "Econocom"),
    ("EURONAV NV", "Euronav"),
    ("EVS BROADCASTING", "EVS Broadcasting"),
    ("EXMAR", "Exmar"),
    ("FAGRON", "Fagron"),
    ("GALAPAGOS", "Galapagos"),
    ("IBA", "IBA"),
    ("KBC GROEP NV", "KBC Groep"),
    ("KINEPOLIS", "Kinepolis"),
    ("MDXHEALTH", "MDxHealth"),
    ("NYRSTAR", "Nyrstar"),
    ("ONTEX", "Ontex"),
    ("ORANGE BELGIUM SA", "Orange Belgium"),
    ("PROXIMUS", "Proximus"),
    ("RECTICEL", "Recticel"),
    ("SOLVAY SA", "Solvay"),
    ("TELENET", "Telenet"),
    ("TESSENDERLO", "Tessenderlo"),
    ("THROMBOGENICS NV", "Thrombogenics"),
    ("TIGENIX", "Tigenix"),
    ("UCB ", "UCB "),
    ("UMICORE", "Umicore")
    ]



con = lite.connect('SummaryDB.db')
cur = con.cursor()

for pretty in prettify:
    pretty_name = pretty[1]
    old_name = pretty[0]
    cur.execute('''UPDATE Issuers SET PRETTY_NAME = "{}" WHERE ISSUER="{}";'''.format(pretty_name,old_name))




con.commit()
cur.close()
con.close()
print("\nDone.\n")





