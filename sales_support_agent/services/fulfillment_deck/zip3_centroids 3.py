"""Approximate centroids for US 3-digit ZIP prefixes.

``ZIP3_CENTROIDS`` maps each assigned 3-digit US ZIP prefix to an
approximate ``(lat, lon)`` centroid. These coordinates are intentionally
coarse (anchored on the dominant city/SCF for the prefix, accurate to
within roughly 50 miles) — they exist solely to drive USPS-style shipping
zone banding via great-circle distance, NOT navigation or geocoding.

Coverage: the 50 states plus DC, Puerto Rico, and the US Virgin Islands.
Military APO/FPO prefixes (090-098, 340, 962-966) and unassigned prefixes
are intentionally omitted; lookups for them return no centroid and the
zone estimator treats them as unknown.
"""

ZIP3_CENTROIDS: dict[str, tuple[float, float]] = {
    "005": (40.81, -73.04),  # Holtsville NY
    "006": (18.4, -66.06),  # San Juan PR
    "007": (18.4, -66.06),  # San Juan PR
    "008": (18.34, -64.93),  # US Virgin Islands
    "009": (18.4, -66.06),  # San Juan PR
    "010": (42.1, -72.59),  # Springfield MA
    "011": (42.1, -72.59),  # Springfield MA
    "012": (42.45, -73.25),  # Pittsfield MA
    "013": (42.59, -72.6),  # Greenfield MA
    "014": (42.58, -71.8),  # Fitchburg MA
    "015": (42.26, -71.8),  # Worcester MA
    "016": (42.26, -71.8),  # Worcester MA
    "017": (42.3, -71.44),  # Framingham MA
    "018": (42.48, -71.15),  # Woburn MA
    "019": (42.47, -70.95),  # Lynn MA
    "020": (42.08, -71.02),  # Brockton MA
    "021": (42.36, -71.06),  # Boston MA
    "022": (42.36, -71.06),  # Boston MA
    "023": (42.08, -71.02),  # Brockton MA
    "024": (42.38, -71.24),  # Waltham MA
    "025": (41.7, -70.3),  # Cape Cod MA
    "026": (41.7, -70.3),  # Cape Cod MA
    "027": (41.64, -70.93),  # New Bedford MA
    "028": (41.82, -71.41),  # Providence RI
    "029": (41.82, -71.41),  # Providence RI
    "030": (42.76, -71.47),  # Nashua NH
    "031": (42.99, -71.46),  # Manchester NH
    "032": (42.99, -71.46),  # Manchester NH
    "033": (43.21, -71.54),  # Concord NH
    "034": (42.93, -72.28),  # Keene NH
    "035": (44.31, -71.77),  # Littleton NH
    "036": (43.24, -72.42),  # Charlestown NH
    "037": (43.37, -72.34),  # Claremont NH
    "038": (43.07, -70.76),  # Portsmouth NH
    "039": (43.09, -70.74),  # Kittery ME
    "040": (43.66, -70.26),  # Portland ME
    "041": (43.66, -70.26),  # Portland ME
    "042": (44.1, -70.21),  # Lewiston ME
    "043": (44.31, -69.78),  # Augusta ME
    "044": (44.8, -68.77),  # Bangor ME
    "045": (43.84, -69.5),  # Mid-coast ME
    "046": (44.72, -67.7),  # Machias ME
    "047": (46.68, -68.01),  # Presque Isle ME
    "048": (44.1, -69.11),  # Rockland ME
    "049": (44.55, -69.63),  # Waterville ME
    "050": (43.65, -72.32),  # White River Junction VT
    "051": (43.13, -72.45),  # Bellows Falls VT
    "052": (42.88, -73.2),  # Bennington VT
    "053": (42.85, -72.56),  # Brattleboro VT
    "054": (44.48, -73.21),  # Burlington VT
    "055": (42.66, -71.14),  # Andover MA
    "056": (44.26, -72.58),  # Montpelier VT
    "057": (43.61, -72.97),  # Rutland VT
    "058": (44.42, -72.02),  # St Johnsbury VT
    "059": (44.92, -71.9),  # Island Pond VT
    "060": (41.76, -72.68),  # Hartford CT
    "061": (41.76, -72.68),  # Hartford CT
    "062": (41.71, -72.21),  # Willimantic CT
    "063": (41.36, -72.1),  # New London CT
    "064": (41.54, -72.81),  # Meriden CT
    "065": (41.31, -72.92),  # New Haven CT
    "066": (41.18, -73.19),  # Bridgeport CT
    "067": (41.56, -73.05),  # Waterbury CT
    "068": (41.09, -73.54),  # Stamford CT
    "069": (41.09, -73.54),  # Stamford CT
    "070": (40.73, -74.17),  # Newark NJ
    "071": (40.73, -74.17),  # Newark NJ
    "072": (40.73, -74.17),  # Newark NJ
    "073": (40.73, -74.17),  # Newark NJ
    "074": (40.92, -74.17),  # Paterson NJ
    "075": (40.92, -74.17),  # Paterson NJ
    "076": (40.89, -74.04),  # Hackensack NJ
    "077": (40.35, -74.07),  # Red Bank NJ
    "078": (40.88, -74.56),  # Dover NJ
    "079": (40.72, -74.36),  # Summit NJ
    "080": (39.9, -75.02),  # Cherry Hill NJ
    "081": (39.93, -75.11),  # Camden NJ
    "082": (39.3, -74.6),  # South Jersey shore NJ
    "083": (39.49, -75.02),  # Vineland NJ
    "084": (39.36, -74.42),  # Atlantic City NJ
    "085": (40.22, -74.76),  # Trenton NJ
    "086": (40.22, -74.76),  # Trenton NJ
    "087": (39.98, -74.2),  # Toms River NJ
    "088": (40.49, -74.45),  # New Brunswick NJ
    "089": (40.49, -74.45),  # New Brunswick NJ
    "100": (40.75, -73.99),  # New York NY (Manhattan)
    "101": (40.75, -73.99),  # New York NY (Manhattan)
    "102": (40.75, -73.99),  # New York NY (Manhattan)
    "103": (40.58, -74.15),  # Staten Island NY
    "104": (40.85, -73.87),  # Bronx NY
    "105": (41.03, -73.76),  # White Plains NY
    "106": (41.03, -73.76),  # White Plains NY
    "107": (40.93, -73.9),  # Yonkers NY
    "108": (40.91, -73.78),  # New Rochelle NY
    "109": (41.15, -74.04),  # Rockland County NY
    "110": (40.72, -73.7),  # Floral Park NY
    "111": (40.74, -73.94),  # Long Island City NY
    "112": (40.65, -73.95),  # Brooklyn NY
    "113": (40.76, -73.81),  # Flushing NY
    "114": (40.7, -73.79),  # Jamaica NY
    "115": (40.75, -73.64),  # Mineola NY
    "116": (40.6, -73.75),  # Far Rockaway NY
    "117": (40.79, -73.25),  # Mid-Suffolk NY
    "118": (40.77, -73.53),  # Hicksville NY
    "119": (40.92, -72.66),  # Riverhead NY
    "120": (42.79, -73.67),  # Troy NY
    "121": (42.65, -73.75),  # Albany NY
    "122": (42.65, -73.75),  # Albany NY
    "123": (42.81, -73.94),  # Schenectady NY
    "124": (41.93, -74.02),  # Kingston NY
    "125": (41.7, -73.92),  # Poughkeepsie NY
    "126": (41.7, -73.92),  # Poughkeepsie NY
    "127": (41.66, -74.69),  # Monticello NY
    "128": (43.31, -73.64),  # Glens Falls NY
    "129": (44.7, -73.45),  # Plattsburgh NY
    "130": (43.05, -76.15),  # Syracuse NY
    "131": (43.05, -76.15),  # Syracuse NY
    "132": (43.05, -76.15),  # Syracuse NY
    "133": (43.1, -75.23),  # Utica NY
    "134": (43.1, -75.23),  # Utica NY
    "135": (43.1, -75.23),  # Utica NY
    "136": (43.97, -75.91),  # Watertown NY
    "137": (42.1, -75.91),  # Binghamton NY
    "138": (42.1, -75.91),  # Binghamton NY
    "139": (42.1, -75.91),  # Binghamton NY
    "140": (42.89, -78.88),  # Buffalo NY
    "141": (42.89, -78.88),  # Buffalo NY
    "142": (42.89, -78.88),  # Buffalo NY
    "143": (43.1, -79.04),  # Niagara Falls NY
    "144": (43.16, -77.61),  # Rochester NY
    "145": (43.16, -77.61),  # Rochester NY
    "146": (43.16, -77.61),  # Rochester NY
    "147": (42.1, -79.24),  # Jamestown NY
    "148": (42.09, -76.8),  # Elmira NY
    "149": (42.09, -76.8),  # Elmira NY
    "150": (40.44, -79.99),  # Pittsburgh PA
    "151": (40.44, -79.99),  # Pittsburgh PA
    "152": (40.44, -79.99),  # Pittsburgh PA
    "153": (40.17, -80.25),  # Washington PA
    "154": (39.9, -79.72),  # Uniontown PA
    "155": (40.01, -79.08),  # Somerset PA
    "156": (40.3, -79.54),  # Greensburg PA
    "157": (40.62, -79.15),  # Indiana PA
    "158": (41.12, -78.76),  # DuBois PA
    "159": (40.33, -78.92),  # Johnstown PA
    "160": (40.86, -79.9),  # Butler PA
    "161": (41.0, -80.35),  # New Castle PA
    "162": (40.83, -79.52),  # Kittanning PA
    "163": (41.43, -79.71),  # Oil City PA
    "164": (42.13, -80.09),  # Erie PA
    "165": (42.13, -80.09),  # Erie PA
    "166": (40.52, -78.4),  # Altoona PA
    "167": (41.96, -78.65),  # Bradford PA
    "168": (40.79, -77.86),  # State College PA
    "169": (41.74, -77.3),  # Wellsboro PA
    "170": (40.27, -76.88),  # Harrisburg PA
    "171": (40.27, -76.88),  # Harrisburg PA
    "172": (39.94, -77.66),  # Chambersburg PA
    "173": (39.96, -76.73),  # York PA
    "174": (39.96, -76.73),  # York PA
    "175": (40.04, -76.31),  # Lancaster PA
    "176": (40.04, -76.31),  # Lancaster PA
    "177": (41.24, -77.0),  # Williamsport PA
    "178": (40.86, -76.79),  # Sunbury PA
    "179": (40.69, -76.2),  # Pottsville PA
    "180": (40.61, -75.47),  # Allentown PA
    "181": (40.61, -75.47),  # Allentown PA
    "182": (40.96, -75.97),  # Hazleton PA
    "183": (41.0, -75.18),  # East Stroudsburg PA
    "184": (41.41, -75.66),  # Scranton PA
    "185": (41.41, -75.66),  # Scranton PA
    "186": (41.25, -75.88),  # Wilkes-Barre PA
    "187": (41.25, -75.88),  # Wilkes-Barre PA
    "188": (41.83, -75.88),  # Montrose PA
    "189": (40.31, -75.13),  # Doylestown PA
    "190": (39.95, -75.16),  # Philadelphia PA
    "191": (39.95, -75.16),  # Philadelphia PA
    "192": (39.95, -75.16),  # Philadelphia PA
    "193": (39.96, -75.61),  # West Chester PA
    "194": (40.12, -75.34),  # Norristown PA
    "195": (40.34, -75.93),  # Reading PA
    "196": (40.34, -75.93),  # Reading PA
    "197": (39.68, -75.75),  # Newark DE
    "198": (39.74, -75.55),  # Wilmington DE
    "199": (39.16, -75.52),  # Dover DE
    "200": (38.9, -77.03),  # Washington DC
    "201": (38.96, -77.45),  # Dulles VA
    "202": (38.9, -77.03),  # Washington DC
    "203": (38.9, -77.03),  # Washington DC
    "204": (38.9, -77.03),  # Washington DC
    "205": (38.9, -77.03),  # Washington DC
    "206": (38.62, -76.91),  # Waldorf MD
    "207": (38.95, -76.85),  # Lanham MD
    "208": (39.02, -77.1),  # Bethesda/Silver Spring MD
    "209": (39.02, -77.1),  # Bethesda/Silver Spring MD
    "210": (39.29, -76.61),  # Baltimore MD
    "211": (39.29, -76.61),  # Baltimore MD
    "212": (39.29, -76.61),  # Baltimore MD
    "214": (38.97, -76.5),  # Annapolis MD
    "215": (39.65, -78.76),  # Cumberland MD
    "216": (38.77, -76.07),  # Easton MD
    "217": (39.41, -77.41),  # Frederick MD
    "218": (38.36, -75.6),  # Salisbury MD
    "219": (39.61, -75.83),  # Elkton MD
    "220": (38.85, -77.3),  # Fairfax VA
    "221": (38.85, -77.3),  # Fairfax VA
    "222": (38.88, -77.1),  # Arlington VA
    "223": (38.82, -77.08),  # Alexandria VA
    "224": (38.3, -77.46),  # Fredericksburg VA
    "225": (38.3, -77.46),  # Fredericksburg VA
    "226": (39.18, -78.16),  # Winchester VA
    "227": (38.47, -77.99),  # Culpeper VA
    "228": (38.45, -78.87),  # Harrisonburg VA
    "229": (38.03, -78.48),  # Charlottesville VA
    "230": (37.54, -77.44),  # Richmond VA
    "231": (37.54, -77.44),  # Richmond VA
    "232": (37.54, -77.44),  # Richmond VA
    "233": (36.85, -76.29),  # Norfolk VA
    "234": (36.85, -76.29),  # Norfolk VA
    "235": (36.85, -76.29),  # Norfolk VA
    "236": (37.05, -76.45),  # Newport News VA
    "237": (36.84, -76.34),  # Portsmouth VA
    "238": (37.21, -77.4),  # Petersburg VA
    "239": (37.3, -78.4),  # Farmville VA
    "240": (37.27, -79.94),  # Roanoke VA
    "241": (37.27, -79.94),  # Roanoke VA
    "242": (36.61, -82.18),  # Bristol VA
    "243": (36.95, -80.92),  # Galax/Pulaski VA
    "244": (38.15, -79.07),  # Staunton VA
    "245": (37.41, -79.14),  # Lynchburg VA
    "246": (37.3, -81.3),  # Tazewell VA
    "247": (37.4, -81.5),  # Bluefield/Welch WV
    "248": (37.4, -81.5),  # Bluefield/Welch WV
    "249": (37.8, -80.45),  # Lewisburg WV
    "250": (38.35, -81.63),  # Charleston WV
    "251": (38.35, -81.63),  # Charleston WV
    "252": (38.35, -81.63),  # Charleston WV
    "253": (38.35, -81.63),  # Charleston WV
    "254": (39.46, -77.96),  # Martinsburg WV
    "255": (38.42, -82.45),  # Huntington WV
    "256": (38.42, -82.45),  # Huntington WV
    "257": (38.42, -82.45),  # Huntington WV
    "258": (37.78, -81.19),  # Beckley WV
    "259": (37.78, -81.19),  # Beckley WV
    "260": (40.06, -80.72),  # Wheeling WV
    "261": (39.27, -81.56),  # Parkersburg WV
    "262": (38.99, -80.23),  # Buckhannon WV
    "263": (39.28, -80.34),  # Clarksburg WV
    "264": (39.28, -80.34),  # Clarksburg WV
    "265": (39.49, -80.14),  # Fairmont WV
    "266": (38.67, -80.71),  # Gassaway WV
    "267": (39.34, -78.76),  # Romney WV
    "268": (38.99, -79.12),  # Petersburg WV
    "270": (36.3, -80.3),  # Mount Airy NC
    "271": (36.1, -80.24),  # Winston-Salem NC
    "272": (36.07, -79.79),  # Greensboro NC
    "273": (36.07, -79.79),  # Greensboro NC
    "274": (36.07, -79.79),  # Greensboro NC
    "275": (35.78, -78.64),  # Raleigh NC
    "276": (35.78, -78.64),  # Raleigh NC
    "277": (35.99, -78.9),  # Durham NC
    "278": (35.94, -77.79),  # Rocky Mount NC
    "279": (36.3, -76.22),  # Elizabeth City NC
    "280": (35.23, -80.84),  # Charlotte NC
    "281": (35.23, -80.84),  # Charlotte NC
    "282": (35.23, -80.84),  # Charlotte NC
    "283": (35.05, -78.88),  # Fayetteville NC
    "284": (34.3, -78.3),  # Wilmington NC
    "285": (35.27, -77.58),  # Kinston NC
    "286": (35.73, -81.34),  # Hickory NC
    "287": (35.6, -82.55),  # Asheville NC
    "288": (35.6, -82.55),  # Asheville NC
    "289": (35.6, -82.55),  # Asheville NC
    "290": (34.0, -81.03),  # Columbia SC
    "291": (34.0, -81.03),  # Columbia SC
    "292": (34.0, -81.03),  # Columbia SC
    "293": (34.2, -82.16),  # Greenwood SC
    "294": (32.78, -79.93),  # Charleston SC
    "295": (34.2, -79.77),  # Florence SC
    "296": (34.85, -82.4),  # Greenville SC
    "297": (34.92, -81.03),  # Rock Hill SC
    "298": (33.56, -81.72),  # Aiken SC
    "299": (32.43, -80.67),  # Beaufort SC
    "300": (33.75, -84.39),  # Atlanta GA
    "301": (33.75, -84.39),  # Atlanta GA
    "302": (33.75, -84.39),  # Atlanta GA
    "303": (33.75, -84.39),  # Atlanta GA
    "304": (32.59, -82.33),  # Swainsboro GA
    "305": (34.3, -83.8),  # Gainesville GA
    "306": (33.96, -83.38),  # Athens GA
    "307": (34.77, -84.97),  # Dalton GA
    "308": (33.47, -81.97),  # Augusta GA
    "309": (33.47, -81.97),  # Augusta GA
    "310": (32.84, -83.63),  # Macon GA
    "311": (32.84, -83.63),  # Macon GA
    "312": (32.84, -83.63),  # Macon GA
    "313": (32.08, -81.1),  # Savannah GA
    "314": (32.08, -81.1),  # Savannah GA
    "315": (31.21, -82.35),  # Waycross GA
    "316": (30.83, -83.28),  # Valdosta GA
    "317": (31.58, -84.16),  # Albany GA
    "318": (32.46, -84.99),  # Columbus GA
    "319": (32.46, -84.99),  # Columbus GA
    "320": (30.33, -81.66),  # Jacksonville FL
    "321": (29.21, -81.02),  # Daytona Beach FL
    "322": (30.33, -81.66),  # Jacksonville FL
    "323": (30.44, -84.28),  # Tallahassee FL
    "324": (30.16, -85.66),  # Panama City FL
    "325": (30.42, -87.22),  # Pensacola FL
    "326": (29.65, -82.32),  # Gainesville FL
    "327": (28.81, -81.27),  # Sanford FL
    "328": (28.54, -81.38),  # Orlando FL
    "329": (28.08, -80.61),  # Melbourne FL
    "330": (25.77, -80.19),  # Miami FL
    "331": (25.77, -80.19),  # Miami FL
    "332": (25.77, -80.19),  # Miami FL
    "333": (26.12, -80.14),  # Fort Lauderdale FL
    "334": (26.71, -80.05),  # West Palm Beach FL
    "335": (27.95, -82.46),  # Tampa FL
    "336": (27.95, -82.46),  # Tampa FL
    "337": (27.77, -82.64),  # St Petersburg FL
    "338": (28.04, -81.95),  # Lakeland FL
    "339": (26.64, -81.87),  # Fort Myers FL
    "341": (26.14, -81.79),  # Naples FL
    "342": (27.34, -82.54),  # Sarasota FL
    "344": (29.19, -82.13),  # Ocala FL
    "346": (28.55, -82.39),  # Brooksville FL
    "347": (28.3, -81.42),  # Kissimmee FL
    "349": (27.24, -80.83),  # Okeechobee FL
    "350": (33.52, -86.8),  # Birmingham AL
    "351": (33.52, -86.8),  # Birmingham AL
    "352": (33.52, -86.8),  # Birmingham AL
    "354": (33.21, -87.57),  # Tuscaloosa AL
    "355": (33.83, -87.28),  # Jasper AL
    "356": (34.61, -86.98),  # Decatur AL
    "357": (34.73, -86.59),  # Huntsville AL
    "358": (34.73, -86.59),  # Huntsville AL
    "359": (34.01, -86.01),  # Gadsden AL
    "360": (32.37, -86.3),  # Montgomery AL
    "361": (32.37, -86.3),  # Montgomery AL
    "362": (33.66, -85.83),  # Anniston AL
    "363": (31.22, -85.39),  # Dothan AL
    "364": (31.43, -86.95),  # Evergreen AL
    "365": (30.69, -88.04),  # Mobile AL
    "366": (30.69, -88.04),  # Mobile AL
    "367": (32.41, -87.02),  # Selma AL
    "368": (32.65, -85.38),  # Opelika AL
    "369": (32.1, -88.21),  # Butler AL
    "370": (36.16, -86.78),  # Nashville TN
    "371": (36.16, -86.78),  # Nashville TN
    "372": (36.16, -86.78),  # Nashville TN
    "373": (35.05, -85.31),  # Chattanooga TN
    "374": (35.05, -85.31),  # Chattanooga TN
    "375": (35.15, -90.05),  # Memphis TN
    "376": (36.31, -82.35),  # Johnson City TN
    "377": (35.96, -83.92),  # Knoxville TN
    "378": (35.96, -83.92),  # Knoxville TN
    "379": (35.96, -83.92),  # Knoxville TN
    "380": (35.15, -90.05),  # Memphis TN
    "381": (35.15, -90.05),  # Memphis TN
    "382": (36.13, -88.51),  # McKenzie TN
    "383": (35.61, -88.81),  # Jackson TN
    "384": (35.62, -87.04),  # Columbia TN
    "385": (36.16, -85.5),  # Cookeville TN
    "386": (34.82, -89.99),  # Hernando MS
    "387": (33.41, -91.06),  # Greenville MS
    "388": (34.26, -88.7),  # Tupelo MS
    "389": (33.52, -90.18),  # Greenwood MS
    "390": (32.3, -90.18),  # Jackson MS
    "391": (32.3, -90.18),  # Jackson MS
    "392": (32.3, -90.18),  # Jackson MS
    "393": (32.36, -88.7),  # Meridian MS
    "394": (31.33, -89.29),  # Hattiesburg MS
    "395": (30.37, -89.09),  # Gulfport MS
    "396": (31.24, -90.45),  # McComb MS
    "397": (33.5, -88.43),  # Columbus MS
    "398": (31.58, -84.16),  # Albany GA
    "399": (33.75, -84.39),  # Atlanta GA
    "400": (38.25, -85.76),  # Louisville KY
    "401": (38.25, -85.76),  # Louisville KY
    "402": (38.25, -85.76),  # Louisville KY
    "403": (38.05, -84.5),  # Lexington KY
    "404": (38.05, -84.5),  # Lexington KY
    "405": (38.05, -84.5),  # Lexington KY
    "406": (38.2, -84.87),  # Frankfort KY
    "407": (37.13, -84.08),  # London KY
    "408": (37.13, -84.08),  # London KY
    "409": (37.13, -84.08),  # London KY
    "410": (39.02, -84.53),  # Covington KY
    "411": (38.48, -82.64),  # Ashland KY
    "412": (38.48, -82.64),  # Ashland KY
    "413": (37.7, -83.5),  # Campton KY
    "414": (37.7, -83.5),  # Campton KY
    "415": (37.48, -82.52),  # Pikeville KY
    "416": (37.48, -82.52),  # Pikeville KY
    "417": (37.25, -83.19),  # Hazard KY
    "418": (37.25, -83.19),  # Hazard KY
    "420": (37.08, -88.6),  # Paducah KY
    "421": (36.99, -86.44),  # Bowling Green KY
    "422": (36.99, -86.44),  # Bowling Green KY
    "423": (37.77, -87.11),  # Owensboro KY
    "424": (37.84, -87.59),  # Henderson KY
    "425": (37.09, -84.6),  # Somerset KY
    "426": (37.09, -84.6),  # Somerset KY
    "427": (37.69, -85.86),  # Elizabethtown KY
    "430": (39.96, -83.0),  # Columbus OH
    "431": (39.96, -83.0),  # Columbus OH
    "432": (39.96, -83.0),  # Columbus OH
    "433": (40.59, -83.13),  # Marion OH
    "434": (41.65, -83.54),  # Toledo OH
    "435": (41.65, -83.54),  # Toledo OH
    "436": (41.65, -83.54),  # Toledo OH
    "437": (39.94, -82.01),  # Zanesville OH
    "438": (39.94, -82.01),  # Zanesville OH
    "439": (40.37, -80.63),  # Steubenville OH
    "440": (41.5, -81.7),  # Cleveland OH
    "441": (41.5, -81.7),  # Cleveland OH
    "442": (41.08, -81.52),  # Akron OH
    "443": (41.08, -81.52),  # Akron OH
    "444": (41.1, -80.65),  # Youngstown OH
    "445": (41.1, -80.65),  # Youngstown OH
    "446": (40.8, -81.38),  # Canton OH
    "447": (40.8, -81.38),  # Canton OH
    "448": (40.76, -82.52),  # Mansfield OH
    "449": (40.76, -82.52),  # Mansfield OH
    "450": (39.1, -84.51),  # Cincinnati OH
    "451": (39.1, -84.51),  # Cincinnati OH
    "452": (39.1, -84.51),  # Cincinnati OH
    "453": (39.76, -84.19),  # Dayton OH
    "454": (39.76, -84.19),  # Dayton OH
    "455": (39.76, -84.19),  # Dayton OH
    "456": (39.33, -82.98),  # Chillicothe OH
    "457": (39.33, -82.1),  # Athens OH
    "458": (40.74, -84.11),  # Lima OH
    "460": (39.77, -86.16),  # Indianapolis IN
    "461": (39.77, -86.16),  # Indianapolis IN
    "462": (39.77, -86.16),  # Indianapolis IN
    "463": (41.6, -87.34),  # Gary IN
    "464": (41.6, -87.34),  # Gary IN
    "465": (41.68, -86.25),  # South Bend IN
    "466": (41.68, -86.25),  # South Bend IN
    "467": (41.08, -85.14),  # Fort Wayne IN
    "468": (41.08, -85.14),  # Fort Wayne IN
    "469": (40.49, -86.13),  # Kokomo IN
    "470": (39.1, -84.85),  # Lawrenceburg IN
    "471": (38.29, -85.82),  # New Albany IN
    "472": (39.2, -85.92),  # Columbus IN
    "473": (40.19, -85.39),  # Muncie IN
    "474": (39.17, -86.53),  # Bloomington IN
    "475": (38.66, -87.17),  # Washington IN
    "476": (37.97, -87.57),  # Evansville IN
    "477": (37.97, -87.57),  # Evansville IN
    "478": (39.47, -87.41),  # Terre Haute IN
    "479": (40.42, -86.89),  # Lafayette IN
    "480": (42.49, -83.14),  # Royal Oak MI
    "481": (42.33, -83.05),  # Detroit MI
    "482": (42.33, -83.05),  # Detroit MI
    "483": (42.64, -83.29),  # Pontiac MI
    "484": (43.01, -83.69),  # Flint MI
    "485": (43.01, -83.69),  # Flint MI
    "486": (43.42, -83.95),  # Saginaw MI
    "487": (43.42, -83.95),  # Saginaw MI
    "488": (42.73, -84.55),  # Lansing MI
    "489": (42.73, -84.55),  # Lansing MI
    "490": (42.29, -85.59),  # Kalamazoo MI
    "491": (42.29, -85.59),  # Kalamazoo MI
    "492": (42.25, -84.4),  # Jackson MI
    "493": (42.96, -85.66),  # Grand Rapids MI
    "494": (42.96, -85.66),  # Grand Rapids MI
    "495": (42.96, -85.66),  # Grand Rapids MI
    "496": (44.76, -85.62),  # Traverse City MI
    "497": (45.03, -84.67),  # Gaylord MI
    "498": (45.82, -88.07),  # Iron Mountain MI
    "499": (45.82, -88.07),  # Iron Mountain MI
    "500": (41.59, -93.62),  # Des Moines IA
    "501": (41.59, -93.62),  # Des Moines IA
    "502": (41.59, -93.62),  # Des Moines IA
    "503": (41.59, -93.62),  # Des Moines IA
    "504": (43.15, -93.2),  # Mason City IA
    "505": (42.5, -94.18),  # Fort Dodge IA
    "506": (42.49, -92.34),  # Waterloo IA
    "507": (42.49, -92.34),  # Waterloo IA
    "508": (41.06, -94.36),  # Creston IA
    "510": (42.5, -96.4),  # Sioux City IA
    "511": (42.5, -96.4),  # Sioux City IA
    "512": (43.18, -95.86),  # Sheldon IA
    "513": (43.15, -95.14),  # Spencer IA
    "514": (42.07, -94.87),  # Carroll IA
    "515": (41.26, -95.85),  # Council Bluffs IA
    "516": (40.77, -95.37),  # Shenandoah IA
    "520": (42.5, -90.66),  # Dubuque IA
    "521": (43.3, -91.8),  # Decorah IA
    "522": (41.98, -91.66),  # Cedar Rapids IA
    "523": (41.98, -91.66),  # Cedar Rapids IA
    "524": (41.98, -91.66),  # Cedar Rapids IA
    "525": (41.02, -92.41),  # Ottumwa IA
    "526": (40.81, -91.11),  # Burlington IA
    "527": (41.52, -90.58),  # Davenport IA
    "528": (41.52, -90.58),  # Davenport IA
    "530": (43.04, -87.91),  # Milwaukee WI
    "531": (43.04, -87.91),  # Milwaukee WI
    "532": (43.04, -87.91),  # Milwaukee WI
    "534": (42.73, -87.78),  # Racine WI
    "535": (43.07, -89.4),  # Madison WI
    "537": (43.07, -89.4),  # Madison WI
    "538": (42.85, -90.71),  # Lancaster WI
    "539": (43.55, -89.46),  # Portage WI
    "540": (45.12, -92.54),  # New Richmond WI
    "541": (44.51, -88.01),  # Green Bay WI
    "542": (44.51, -88.01),  # Green Bay WI
    "543": (44.51, -88.01),  # Green Bay WI
    "544": (44.96, -89.63),  # Wausau WI
    "545": (45.64, -89.41),  # Rhinelander WI
    "546": (43.8, -91.24),  # La Crosse WI
    "547": (44.81, -91.5),  # Eau Claire WI
    "548": (45.82, -91.89),  # Spooner WI
    "549": (44.02, -88.54),  # Oshkosh WI
    "550": (44.95, -93.09),  # St Paul MN
    "551": (44.95, -93.09),  # St Paul MN
    "553": (44.98, -93.27),  # Minneapolis MN
    "554": (44.98, -93.27),  # Minneapolis MN
    "555": (44.98, -93.27),  # Minneapolis MN
    "556": (46.79, -92.1),  # Duluth MN
    "557": (46.79, -92.1),  # Duluth MN
    "558": (46.79, -92.1),  # Duluth MN
    "559": (44.02, -92.47),  # Rochester MN
    "560": (44.16, -94.0),  # Mankato MN
    "561": (43.87, -95.12),  # Windom MN
    "562": (45.12, -95.04),  # Willmar MN
    "563": (45.56, -94.16),  # St Cloud MN
    "564": (46.36, -94.2),  # Brainerd MN
    "565": (46.82, -95.85),  # Detroit Lakes MN
    "566": (47.47, -94.88),  # Bemidji MN
    "567": (48.12, -96.18),  # Thief River Falls MN
    "570": (43.55, -96.7),  # Sioux Falls SD
    "571": (43.55, -96.7),  # Sioux Falls SD
    "572": (44.9, -97.12),  # Watertown SD
    "573": (43.71, -98.03),  # Mitchell SD
    "574": (45.46, -98.49),  # Aberdeen SD
    "575": (44.37, -100.35),  # Pierre SD
    "576": (45.54, -100.43),  # Mobridge SD
    "577": (44.08, -103.23),  # Rapid City SD
    "580": (46.88, -96.79),  # Fargo ND
    "581": (46.88, -96.79),  # Fargo ND
    "582": (47.93, -97.03),  # Grand Forks ND
    "583": (48.11, -98.86),  # Devils Lake ND
    "584": (46.91, -98.7),  # Jamestown ND
    "585": (46.81, -100.78),  # Bismarck ND
    "586": (46.88, -102.79),  # Dickinson ND
    "587": (48.23, -101.3),  # Minot ND
    "588": (48.15, -103.62),  # Williston ND
    "590": (45.78, -108.5),  # Billings MT
    "591": (45.78, -108.5),  # Billings MT
    "592": (48.09, -105.64),  # Wolf Point MT
    "593": (46.41, -105.84),  # Miles City MT
    "594": (47.5, -111.3),  # Great Falls MT
    "595": (48.55, -109.68),  # Havre MT
    "596": (46.59, -112.04),  # Helena MT
    "597": (46.0, -112.53),  # Butte MT
    "598": (46.87, -113.99),  # Missoula MT
    "599": (48.2, -114.31),  # Kalispell MT
    "600": (42.11, -88.03),  # Palatine IL
    "601": (41.91, -88.13),  # Carol Stream IL
    "602": (42.05, -87.69),  # Evanston IL
    "603": (41.89, -87.79),  # Oak Park IL
    "604": (41.5, -87.6),  # South Suburban IL
    "605": (41.76, -88.32),  # Aurora IL
    "606": (41.88, -87.63),  # Chicago IL
    "607": (41.88, -87.63),  # Chicago IL
    "608": (41.88, -87.63),  # Chicago IL
    "609": (41.12, -87.86),  # Kankakee IL
    "610": (42.27, -89.09),  # Rockford IL
    "611": (42.27, -89.09),  # Rockford IL
    "612": (41.49, -90.57),  # Rock Island IL
    "613": (41.33, -89.09),  # La Salle IL
    "614": (40.95, -90.37),  # Galesburg IL
    "615": (40.69, -89.59),  # Peoria IL
    "616": (40.69, -89.59),  # Peoria IL
    "617": (40.48, -88.99),  # Bloomington IL
    "618": (40.12, -88.24),  # Champaign IL
    "619": (40.12, -88.24),  # Champaign IL
    "620": (38.7, -90.15),  # Granite City IL
    "622": (38.62, -90.16),  # East St Louis IL
    "623": (39.94, -91.41),  # Quincy IL
    "624": (39.12, -88.54),  # Effingham IL
    "625": (39.78, -89.65),  # Springfield IL
    "626": (39.78, -89.65),  # Springfield IL
    "627": (39.78, -89.65),  # Springfield IL
    "628": (38.53, -89.13),  # Centralia IL
    "629": (37.73, -89.22),  # Carbondale IL
    "630": (38.63, -90.2),  # St Louis MO
    "631": (38.63, -90.2),  # St Louis MO
    "633": (38.79, -90.48),  # St Charles MO
    "634": (39.7, -91.36),  # Hannibal MO
    "635": (40.19, -92.58),  # Kirksville MO
    "636": (37.85, -90.52),  # Park Hills MO
    "637": (37.31, -89.52),  # Cape Girardeau MO
    "638": (36.88, -89.59),  # Sikeston MO
    "639": (36.76, -90.39),  # Poplar Bluff MO
    "640": (39.1, -94.58),  # Kansas City MO
    "641": (39.1, -94.58),  # Kansas City MO
    "644": (39.77, -94.85),  # St Joseph MO
    "645": (39.77, -94.85),  # St Joseph MO
    "646": (39.79, -93.55),  # Chillicothe MO
    "647": (38.65, -94.35),  # Harrisonville MO
    "648": (37.08, -94.51),  # Joplin MO
    "650": (38.58, -92.17),  # Jefferson City MO
    "651": (38.58, -92.17),  # Jefferson City MO
    "652": (38.95, -92.33),  # Columbia MO
    "653": (38.7, -93.23),  # Sedalia MO
    "654": (37.21, -93.29),  # Springfield MO
    "655": (37.21, -93.29),  # Springfield MO
    "656": (37.21, -93.29),  # Springfield MO
    "657": (37.21, -93.29),  # Springfield MO
    "658": (37.21, -93.29),  # Springfield MO
    "660": (39.11, -94.63),  # Kansas City KS
    "661": (39.11, -94.63),  # Kansas City KS
    "662": (39.11, -94.63),  # Kansas City KS
    "664": (39.05, -95.68),  # Topeka KS
    "665": (39.05, -95.68),  # Topeka KS
    "666": (39.05, -95.68),  # Topeka KS
    "667": (37.84, -94.71),  # Fort Scott KS
    "668": (39.05, -95.68),  # Topeka KS
    "670": (37.69, -97.34),  # Wichita KS
    "671": (37.69, -97.34),  # Wichita KS
    "672": (37.69, -97.34),  # Wichita KS
    "673": (37.22, -95.71),  # Independence KS
    "674": (38.84, -97.61),  # Salina KS
    "675": (38.06, -97.93),  # Hutchinson KS
    "676": (38.88, -99.33),  # Hays KS
    "677": (39.4, -101.05),  # Colby KS
    "678": (37.75, -100.02),  # Dodge City KS
    "679": (37.04, -100.92),  # Liberal KS
    "680": (41.26, -95.93),  # Omaha NE
    "681": (41.26, -95.93),  # Omaha NE
    "683": (40.81, -96.68),  # Lincoln NE
    "684": (40.81, -96.68),  # Lincoln NE
    "685": (40.81, -96.68),  # Lincoln NE
    "686": (41.43, -97.37),  # Columbus NE
    "687": (42.03, -97.42),  # Norfolk NE
    "688": (40.93, -98.34),  # Grand Island NE
    "689": (40.59, -98.39),  # Hastings NE
    "690": (40.2, -100.63),  # McCook NE
    "691": (41.12, -100.77),  # North Platte NE
    "692": (42.87, -100.55),  # Valentine NE
    "693": (42.1, -102.87),  # Alliance NE
    "700": (29.95, -90.07),  # New Orleans LA
    "701": (29.95, -90.07),  # New Orleans LA
    "703": (29.79, -90.82),  # Thibodaux LA
    "704": (30.5, -90.46),  # Hammond LA
    "705": (30.22, -92.02),  # Lafayette LA
    "706": (30.23, -93.22),  # Lake Charles LA
    "707": (30.45, -91.15),  # Baton Rouge LA
    "708": (30.45, -91.15),  # Baton Rouge LA
    "710": (32.52, -93.75),  # Shreveport LA
    "711": (32.52, -93.75),  # Shreveport LA
    "712": (32.51, -92.12),  # Monroe LA
    "713": (31.31, -92.44),  # Alexandria LA
    "714": (31.31, -92.44),  # Alexandria LA
    "716": (34.22, -92.0),  # Pine Bluff AR
    "717": (33.58, -92.83),  # Camden AR
    "718": (33.44, -94.04),  # Texarkana AR
    "719": (34.5, -93.06),  # Hot Springs AR
    "720": (34.75, -92.29),  # Little Rock AR
    "721": (34.75, -92.29),  # Little Rock AR
    "722": (34.75, -92.29),  # Little Rock AR
    "723": (35.15, -90.18),  # West Memphis AR
    "724": (35.84, -90.7),  # Jonesboro AR
    "725": (35.77, -91.64),  # Batesville AR
    "726": (36.23, -93.11),  # Harrison AR
    "727": (36.06, -94.16),  # Fayetteville AR
    "728": (35.28, -93.13),  # Russellville AR
    "729": (35.39, -94.4),  # Fort Smith AR
    "730": (35.47, -97.52),  # Oklahoma City OK
    "731": (35.47, -97.52),  # Oklahoma City OK
    "734": (34.17, -97.13),  # Ardmore OK
    "735": (34.61, -98.4),  # Lawton OK
    "736": (35.51, -98.97),  # Clinton OK
    "737": (36.4, -97.88),  # Enid OK
    "738": (36.43, -99.39),  # Woodward OK
    "739": (36.68, -101.48),  # Guymon OK
    "740": (36.15, -95.99),  # Tulsa OK
    "741": (36.15, -95.99),  # Tulsa OK
    "743": (36.15, -95.99),  # Tulsa OK
    "744": (35.75, -95.37),  # Muskogee OK
    "745": (34.93, -95.77),  # McAlester OK
    "746": (36.71, -97.09),  # Ponca City OK
    "747": (33.99, -96.4),  # Durant OK
    "748": (35.33, -96.93),  # Shawnee OK
    "749": (35.05, -94.62),  # Poteau OK
    "750": (32.78, -96.8),  # Dallas TX
    "751": (32.78, -96.8),  # Dallas TX
    "752": (32.78, -96.8),  # Dallas TX
    "753": (32.78, -96.8),  # Dallas TX
    "754": (33.14, -96.11),  # Greenville TX
    "755": (33.43, -94.05),  # Texarkana TX
    "756": (32.5, -94.74),  # Longview TX
    "757": (32.35, -95.3),  # Tyler TX
    "758": (31.76, -95.63),  # Palestine TX
    "759": (31.34, -94.73),  # Lufkin TX
    "760": (32.76, -97.33),  # Fort Worth TX
    "761": (32.76, -97.33),  # Fort Worth TX
    "762": (33.21, -97.13),  # Denton TX
    "763": (33.91, -98.49),  # Wichita Falls TX
    "764": (32.22, -98.2),  # Eastland TX
    "765": (31.1, -97.34),  # Temple TX
    "766": (31.55, -97.15),  # Waco TX
    "767": (31.55, -97.15),  # Waco TX
    "768": (31.71, -98.99),  # Brownwood TX
    "769": (31.46, -100.44),  # San Angelo TX
    "770": (29.76, -95.36),  # Houston TX
    "771": (29.76, -95.36),  # Houston TX
    "772": (29.76, -95.36),  # Houston TX
    "773": (30.31, -95.46),  # Conroe TX
    "774": (29.58, -95.76),  # Richmond TX
    "775": (29.69, -95.21),  # Pasadena TX
    "776": (30.08, -94.1),  # Beaumont TX
    "777": (30.08, -94.1),  # Beaumont TX
    "778": (30.67, -96.37),  # Bryan TX
    "779": (28.81, -97.0),  # Victoria TX
    "780": (29.42, -98.49),  # San Antonio TX
    "781": (29.42, -98.49),  # San Antonio TX
    "782": (29.42, -98.49),  # San Antonio TX
    "783": (27.8, -97.4),  # Corpus Christi TX
    "784": (27.8, -97.4),  # Corpus Christi TX
    "785": (26.2, -98.23),  # McAllen TX
    "786": (30.27, -97.74),  # Austin TX
    "787": (30.27, -97.74),  # Austin TX
    "788": (29.21, -99.79),  # Uvalde TX
    "789": (29.87, -97.94),  # San Marcos TX
    "790": (35.21, -101.83),  # Amarillo TX
    "791": (35.21, -101.83),  # Amarillo TX
    "792": (34.43, -100.25),  # Childress TX
    "793": (33.58, -101.86),  # Lubbock TX
    "794": (33.58, -101.86),  # Lubbock TX
    "795": (32.45, -99.73),  # Abilene TX
    "796": (32.45, -99.73),  # Abilene TX
    "797": (31.99, -102.08),  # Midland TX
    "798": (31.76, -106.49),  # El Paso TX
    "799": (31.76, -106.49),  # El Paso TX
    "800": (39.74, -104.99),  # Denver CO
    "801": (39.74, -104.99),  # Denver CO
    "802": (39.74, -104.99),  # Denver CO
    "803": (40.01, -105.27),  # Boulder CO
    "804": (39.76, -105.22),  # Golden CO
    "805": (40.17, -105.1),  # Longmont CO
    "806": (39.99, -104.82),  # Brighton CO
    "807": (40.25, -103.8),  # Fort Morgan CO
    "808": (38.83, -104.82),  # Colorado Springs CO
    "809": (38.83, -104.82),  # Colorado Springs CO
    "810": (38.25, -104.61),  # Pueblo CO
    "811": (37.47, -105.87),  # Alamosa CO
    "812": (38.53, -106.0),  # Salida CO
    "813": (37.27, -107.88),  # Durango CO
    "814": (39.06, -108.55),  # Grand Junction CO
    "815": (39.06, -108.55),  # Grand Junction CO
    "816": (39.55, -107.32),  # Glenwood Springs CO
    "820": (41.14, -104.82),  # Cheyenne WY
    "822": (42.05, -104.95),  # Wheatland WY
    "823": (41.79, -107.24),  # Rawlins WY
    "824": (44.02, -107.96),  # Worland WY
    "825": (43.02, -108.38),  # Riverton WY
    "826": (42.85, -106.32),  # Casper WY
    "827": (44.29, -105.5),  # Gillette WY
    "828": (44.8, -106.96),  # Sheridan WY
    "829": (41.59, -109.22),  # Rock Springs WY
    "830": (41.59, -109.22),  # Rock Springs WY
    "831": (41.59, -109.22),  # Rock Springs WY
    "832": (42.86, -112.45),  # Pocatello ID
    "833": (42.56, -114.46),  # Twin Falls ID
    "834": (43.49, -112.04),  # Idaho Falls ID
    "835": (46.42, -117.02),  # Lewiston ID
    "836": (43.62, -116.21),  # Boise ID
    "837": (43.62, -116.21),  # Boise ID
    "838": (47.68, -116.78),  # Coeur d'Alene ID
    "840": (40.76, -111.89),  # Salt Lake City UT
    "841": (40.76, -111.89),  # Salt Lake City UT
    "842": (41.22, -111.97),  # Ogden UT
    "843": (41.22, -111.97),  # Ogden UT
    "844": (41.22, -111.97),  # Ogden UT
    "845": (39.6, -110.81),  # Price UT
    "846": (40.23, -111.66),  # Provo UT
    "847": (40.23, -111.66),  # Provo UT
    "850": (33.45, -112.07),  # Phoenix AZ
    "851": (33.45, -112.07),  # Phoenix AZ
    "852": (33.45, -112.07),  # Phoenix AZ
    "853": (33.45, -112.07),  # Phoenix AZ
    "855": (33.39, -110.79),  # Globe AZ
    "856": (32.22, -110.97),  # Tucson AZ
    "857": (32.22, -110.97),  # Tucson AZ
    "859": (34.25, -110.03),  # Show Low AZ
    "860": (35.2, -111.65),  # Flagstaff AZ
    "863": (34.54, -112.47),  # Prescott AZ
    "864": (35.19, -114.05),  # Kingman AZ
    "865": (35.2, -109.3),  # Chambers AZ
    "870": (35.08, -106.65),  # Albuquerque NM
    "871": (35.08, -106.65),  # Albuquerque NM
    "873": (35.53, -108.74),  # Gallup NM
    "874": (36.73, -108.22),  # Farmington NM
    "875": (35.69, -105.94),  # Santa Fe NM
    "877": (35.59, -105.22),  # Las Vegas NM
    "878": (34.06, -106.89),  # Socorro NM
    "879": (33.13, -107.25),  # Truth or Consequences NM
    "880": (32.31, -106.78),  # Las Cruces NM
    "881": (34.4, -103.21),  # Clovis NM
    "882": (33.39, -104.52),  # Roswell NM
    "883": (32.9, -105.96),  # Alamogordo NM
    "884": (35.17, -103.72),  # Tucumcari NM
    "885": (31.76, -106.49),  # El Paso TX
    "889": (36.17, -115.14),  # Las Vegas NV
    "890": (36.17, -115.14),  # Las Vegas NV
    "891": (36.17, -115.14),  # Las Vegas NV
    "893": (39.25, -114.89),  # Ely NV
    "894": (39.53, -119.81),  # Reno NV
    "895": (39.53, -119.81),  # Reno NV
    "897": (39.16, -119.77),  # Carson City NV
    "898": (40.83, -115.76),  # Elko NV
    "900": (34.05, -118.24),  # Los Angeles CA
    "901": (34.05, -118.24),  # Los Angeles CA
    "902": (34.05, -118.24),  # Los Angeles CA
    "903": (33.96, -118.35),  # Inglewood CA
    "904": (34.02, -118.49),  # Santa Monica CA
    "905": (33.84, -118.34),  # Torrance CA
    "906": (33.97, -118.03),  # Whittier CA
    "907": (33.77, -118.19),  # Long Beach CA
    "908": (33.77, -118.19),  # Long Beach CA
    "910": (34.15, -118.2),  # Pasadena/Glendale CA
    "911": (34.15, -118.2),  # Pasadena/Glendale CA
    "912": (34.15, -118.2),  # Pasadena/Glendale CA
    "913": (34.19, -118.45),  # San Fernando Valley CA
    "914": (34.19, -118.45),  # San Fernando Valley CA
    "915": (34.19, -118.45),  # San Fernando Valley CA
    "916": (34.19, -118.45),  # San Fernando Valley CA
    "917": (34.06, -117.97),  # San Gabriel Valley CA
    "918": (34.06, -117.97),  # San Gabriel Valley CA
    "919": (32.72, -117.16),  # San Diego CA
    "920": (32.72, -117.16),  # San Diego CA
    "921": (32.72, -117.16),  # San Diego CA
    "922": (33.72, -116.22),  # Indio/Palm Springs CA
    "923": (34.11, -117.29),  # San Bernardino CA
    "924": (34.11, -117.29),  # San Bernardino CA
    "925": (33.95, -117.4),  # Riverside CA
    "926": (33.69, -117.83),  # Irvine/Santa Ana CA
    "927": (33.69, -117.83),  # Irvine/Santa Ana CA
    "928": (33.84, -117.91),  # Anaheim CA
    "930": (34.2, -119.18),  # Oxnard CA
    "931": (34.42, -119.7),  # Santa Barbara CA
    "932": (35.37, -119.02),  # Bakersfield CA
    "933": (35.37, -119.02),  # Bakersfield CA
    "934": (34.95, -120.43),  # Santa Maria CA
    "935": (35.05, -118.17),  # Mojave CA
    "936": (36.74, -119.79),  # Fresno CA
    "937": (36.74, -119.79),  # Fresno CA
    "938": (36.74, -119.79),  # Fresno CA
    "939": (36.68, -121.66),  # Salinas CA
    "940": (37.77, -122.42),  # San Francisco CA
    "941": (37.77, -122.42),  # San Francisco CA
    "942": (38.58, -121.49),  # Sacramento CA
    "943": (37.44, -122.14),  # Palo Alto CA
    "944": (37.56, -122.32),  # San Mateo CA
    "945": (37.9, -122.06),  # Walnut Creek CA
    "946": (37.8, -122.27),  # Oakland CA
    "947": (37.87, -122.27),  # Berkeley CA
    "948": (37.94, -122.35),  # Richmond CA
    "949": (38.0, -122.53),  # San Rafael CA
    "950": (37.34, -121.89),  # San Jose CA
    "951": (37.34, -121.89),  # San Jose CA
    "952": (37.96, -121.29),  # Stockton CA
    "953": (37.64, -120.99),  # Modesto CA
    "954": (38.44, -122.71),  # Santa Rosa CA
    "955": (40.8, -124.16),  # Eureka CA
    "956": (38.58, -121.49),  # Sacramento CA
    "957": (38.58, -121.49),  # Sacramento CA
    "958": (38.58, -121.49),  # Sacramento CA
    "959": (39.15, -121.59),  # Marysville CA
    "960": (40.59, -122.39),  # Redding CA
    "961": (39.33, -120.18),  # Truckee CA
    "967": (21.31, -157.86),  # Honolulu HI
    "968": (21.31, -157.86),  # Honolulu HI
    "970": (45.52, -122.68),  # Portland OR
    "971": (45.52, -122.68),  # Portland OR
    "972": (45.52, -122.68),  # Portland OR
    "973": (44.94, -123.04),  # Salem OR
    "974": (44.05, -123.09),  # Eugene OR
    "975": (42.33, -122.87),  # Medford OR
    "976": (42.22, -121.78),  # Klamath Falls OR
    "977": (44.06, -121.31),  # Bend OR
    "978": (45.67, -118.79),  # Pendleton OR
    "979": (44.03, -116.96),  # Ontario OR
    "980": (47.61, -122.33),  # Seattle WA
    "981": (47.61, -122.33),  # Seattle WA
    "982": (47.98, -122.2),  # Everett WA
    "983": (47.25, -122.44),  # Tacoma WA
    "984": (47.25, -122.44),  # Tacoma WA
    "985": (47.04, -122.9),  # Olympia WA
    "986": (45.64, -122.6),  # Vancouver WA
    "988": (47.42, -120.31),  # Wenatchee WA
    "989": (46.6, -120.51),  # Yakima WA
    "990": (47.66, -117.43),  # Spokane WA
    "991": (47.66, -117.43),  # Spokane WA
    "992": (47.66, -117.43),  # Spokane WA
    "993": (46.24, -119.1),  # Pasco WA
    "994": (46.42, -117.05),  # Clarkston WA
    "995": (61.22, -149.9),  # Anchorage AK
    "996": (61.58, -149.44),  # Wasilla AK
    "997": (64.84, -147.72),  # Fairbanks AK
    "998": (58.3, -134.42),  # Juneau AK
    "999": (55.34, -131.65),  # Ketchikan AK
}
