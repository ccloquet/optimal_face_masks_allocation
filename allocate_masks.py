#####################################################################################################################################
#
# How to best dispatch tne face masks within the pharmacies of a city                                                               #
# so that the citizen should go to the nearest shop from his/her house                                                              #
#                                                                                                                                   #
# This algorithm assigns each street to the nearest pharmacy,                                                                       #
# then tries to rebalance a bit the number of face masks between the stores                                                         #
#                                                                                                                                   #
# It uses the ICAR database (Service public de Wallonie) to geocode the streets,                                                    #
# and the Google API for the streets that were not found.                                                                           #
# In last resorts, it uses a manually made table to geocode the remaining streets                                                   #
#                                                                                                                                   #
# C. Cloquet (Poppy) - 2020-04-22 - Licence MIT (Free to reuse)                                                                     #
#                                                                                                                                   #
#####################################################################################################################################

import csv
import requests
import json
import math
import tabulate
import time
import numpy
import operator
import json

from    os         import path
from    pyproj     import Proj
from    difflib    import SequenceMatcher
import  unicodedata

GOOGLE_APIKEY           = '';

MAX_STREETS             = 1000;                                         # for debug - max number of streets to use

# source files
pharmacies_geojson      = 'pharmacies_from_kml_lambert72.geojson'       # (converted from a KML using QGIS)
                                                                        # we use
                                                                        # name    = f['properties']['Name']
                                                                        # descr   = f['properties']['description'] # as address
                                                                        # x       = f['geometry']['coordinates'][0]
                                                                        # y       = f['geometry']['coordinates'][1]

rues_csv                = 'rues.csv'                                    # streets list (we use columns 1 (number of people), 5 (street name) and 6 (zip code)
                                                                        # column index starts at 0
                                                                        # with title row
                                                                        # !decimal separator
VILLE                   = 'XXXXX, Belgium'

# city bounding box (in Lambert 72 coordinates)
xmin                    = 0;
xmax                    = 990000;
ymin                    = 0;
ymax                    = 990000;

# to geocode the remaining streets
missing_streets_fname   = '20200422_streets_missing_completed.csv'      # we use columns 0 (name + zip), 1 (latitude) and 2 (longitude)

# parameters for the reequilibration
coeff                   = 1.5
q                       = 100

# two intermediate files
officines_poppy_fname   = "200420_officines.poppy"
rues_poppy_fname        = "200420_rues.poppy"
myProj                  = Proj("+proj=lcc +lat_1=51.16666723333333 +lat_2=49.8333339 +lat_0=90 +lon_0=4.367486666666666 +x_0=150000.013 +y_0=5400088.438 +ellps=intl +towgs84=-106.869,52.2978,-103.724,0.3366,-0.457,1.8422,-1.2747 +units=m +no_defs")

def strip_accents(text):

    try:
        text = unicode(text, 'utf-8')
    except NameError: # unicode is a default on python 3 
        pass

    text = unicodedata.normalize('NFD', text)\
           .encode('ascii', 'ignore')\
           .decode("utf-8")

    return str(text)

def google_find(street):
    
    next_uri        = "https://maps.googleapis.com/maps/api/geocode/json?address=" + street + "&key=" + GOOGLE_APIKEY;

    r               = requests.get(next_uri)
    if (r.status_code != 200):          print('************' +str(r.status_code))

    r_json          = r.json()
    s               = r_json['results'];

    if len(s) == 0:
        return 0, 0, '', 0
        
    s   = s[0];

    lat = s['geometry']['location']['lat']
    lng = s['geometry']['location']['lng']
    nam = strip_accents(s['formatted_address'].replace(', Belgium', ''))

    x, y = myProj(lng, lat)

    score     = SequenceMatcher(None, street.lower().rsplit(' ', 1)[0].strip(), nam.lower().rsplit(' ', 1)[0].strip()).ratio() # .rsplit(' ', 1)[0] -> remove last wor (ie : municipality), but remain zip
    
    return int(x), int(y), nam, score


myofficines         = [];
mystreets           = [];
missing_streets     = [];
                           
# ***********
# officines
# ***********
# if the intermediate file exists, loads it, otherwise build it
if path.exists(officines_poppy_fname):
    with open(officines_poppy_fname, 'r') as filehandle:
        myofficines = json.load(filehandle)
        
else:
    myofficines = []
    myofficines_csv = []

    print('\n>>> building officines list\n')

    with open('200420_Officines_entite_XXX_adapted.csv', 'r') as csvfile:
        officines_filter = csv.reader(csvfile, delimiter=';', quotechar='|')

        next(officines_filter)
        
        for row in officines_filter:
            #print(row[2] + ' ' + row[1])
            #print(row[4] + ' ' + row[5]  + ', ' + row[6] + ' ' + row[7])
            
            tofind = row[4] + ' ' + row[5]  + ', ' + row[6] + ' ' + row[7]

            myid    = hash(tofind)
            
            x, y, name, score = google_find(tofind)

            if score < 1:
                print(x, y, name.lower(), tofind.lower(), score)
            
            myofficines.append({'id':myid, 'name':row[2] + ' ' + row[1], 'descr': tofind,  'x':x, 'y':y})
            myofficines_csv.append([myid, row[2] + ' ' + row[1], tofind,  x, y])

        with open(officines_poppy_fname+'_test.csv', 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(myofficines_csv)

        with open(officines_poppy_fname, 'w') as f:
            json.dump(myofficines, f)
                       
# ***********
# streets
# ***********
# if the intermediate file exists, loads it, otherwise build it
if path.exists(rues_poppy_fname):
    with open(rues_poppy_fname, 'r') as filehandle:
        mystreets = json.load(filehandle)
else:   
    with open(rues_csv, newline='', encoding='utf-8') as csvfile:

        rues      = csv.reader(csvfile, delimiter=';', quotechar='|')
        
        incorrect = 0;
        total     = 0;
        numok     = 0;
        mystreets = [];
        provider  = '.';
        ok        = 0;
        
        print('\n>>> building streets list\n')

        next(rues)                                  # skip title
        for row in rues:
            time.sleep(.25)                          # throttling for the API

            ok      = True
            total   += 1;
            n       = int(row[1])
            rue     = row[5]
            cp      = row[6]

            # look into missing streets
            found   = False;
            tofind  = rue + ' ' + cp
                             
            if path.exists(missing_streets_fname):
                with open(missing_streets_fname, newline='') as csvfile: #, encoding='utf-8'
                    missing_streets = csv.reader(csvfile, delimiter=';', quotechar='|')
                    next(missing_streets)
                    
                    for missing_street in missing_streets:
                        if missing_street[0].strip() == tofind.strip():
                            x, y        = myProj(missing_street[2].replace(',', '.'), missing_street[1].replace(',', '.'))
                            found       = True
                            ok          = True
                            score       = 1
                            provider    = 'M';
                            break;
            
            # if not found, try other means
            if found == False:
                r       = requests.get('http://geoservices.wallonie.be//geolocalisation/rest/getListeRuesByCpAndNomApprox/' + cp + '/' + rue + '/')
                provider = 'W';
                
                if (r.status_code != 200):
                    print('************' +str(r.status_code))

                streets = r.json()

                if (streets['errorMsg'] != None):
                    print('*************' + streets['errorMsg'])

                if (len(streets['rues'])==0):
                    print('!' + rue)
                    ok = False
                    
                for street in streets['rues']:          # aim is to take the first one

                    ok      = True
                    x       = (street['xMin'] + street['xMax'])/2;
                    y       = (street['yMin'] + street['yMax'])/2;
                    score   = street['score'];
                    
                    if score < 100:
                        ok           = False
                        rue_split    = rue.split(' ', 3)

                        # maybe the street name is inverted (ex : rue Rémy Goroges instead of rue Georges Remy) -> tries that
                        if len(rue_split) == 3:
                            inverted_rue     = rue_split[0] + ' ' + rue_split[2] + ' ' + rue_split[1]

                            if inverted_rue.lower() == street['nom'].title().lower():
                                ok      = True

                        # if not, tries via Google
                        if ok == False:
                            tofind    = rue + ', ' + cp + ' ' + VILLE
                            x, y, nam, score = google_find(tofind)
                            provider  = 'G';
                            
                            if tofind.find('oleilmont') > 0:
                                score = 1

                            if score == 1:
                                ok = True
                            else:
                                incorrect   += 1
                                ok          = False

                    break;
                             
            if (x == 0) | (y == 0) | (x < xmin) | (x > xmax) | (y < ymin) | (y > ymax): # city bounding box
                print ('[BB ISSUE]')
                ok = False
                    
            if ok:
                print(provider, end = '')
                numok += 1
                mystreets.append({'rue': rue, 'cp':cp, 'n':n, 'x':x, 'y':y})
            else:
                print ('\n! ' + rue + ' ' + cp + '### Google: ['+ nam + '] ### SPW: [' + street['nom'].title() + '] ## pc incorrect: ' + str(100*incorrect/total));
           
            if total >= MAX_STREETS:
                print('max streets break')
                print(total)
                print(numok)
                break;
            
    with open(rues_poppy_fname, 'w') as filehandle:
        json.dump(mystreets, filehandle)

print('\n>>> which street goes where')

my_groups      = {};
my_streets_idx = {};

for officine in myofficines:
    my_groups[officine['id']] = {'id':officine['id'], 'name':officine['name'], 'descr':officine['descr'], 'x':officine['x'], 'y':officine['y'], 'n':0, 'n0':0, 'done':False, 'list':{}}

# which officine for a given street
Npers = 0
for street in mystreets:
    my_streets_idx[street['rue'] + '_' + street['cp']] = {'orig':'', 'now':''}
    Npers += street['n']

for street in mystreets:
    d20 = 9e99
    t   = street;
    
    for officine in myofficines:
        d2  = (officine['x'] - street['x'])*(officine['x'] - street['x']) + (officine['y'] - street['y'])*(officine['y'] - street['y']);
        if d2 < d20:
            u   = officine
            d20 = d2
        
    # u est l'officine la plus proche de la rue

    off_idx = u['id'];
    str_idx = t['rue'] + '_' + t['cp']
    
    my_streets_idx[str_idx]['orig']     = off_idx
    my_streets_idx[str_idx]['now']      = my_streets_idx[str_idx]['orig']
    my_groups[off_idx]['n']            += t['n']
    my_groups[off_idx]['n0']            = my_groups[off_idx]['n']
    my_groups[off_idx]['list'][str_idx] = t
    
Npers_off   = coeff * Npers / len(myofficines)
w           = []

###########################################
# rebalancing
###########################################

while q > 0:
    q-=1
    for k, v in my_groups.items():
        my_groups[k]['done']         = False
        w.append(my_groups[k]['n'])

    print(str(min(w)) + ' ' + str(numpy.mean(w)) + ' ' + str(max(w)) + ' ' + str(numpy.std(w)))
    
    p = len(myofficines);
    while p > 0:
        p -= 1;
        
        #recherche de la pharma qui a le moins d'items, qui n'a pas encore ete traitée et dont le nombre d'items est plus petit que la moyenne
        off_idx  = ''
        officine = {}
        v0       = 9e99
        
        for k, v in my_groups.items():

            if v['done']:
                continue;

            if v['n'] > Npers_off:
                continue;
            
            if v['n'] < v0:
                off_idx  = k
                officine = v
                v0       = v['n']

        if v0 > 9e98:
            break;
        
        #recherche de la rue la plus proche de cette pharma, qui n'appartient pas encore à cette pharma
        d20 = 9e99
        for street in mystreets:

            str_idx = street['rue'] + '_' + street['cp']
            
            # recherche seulement dans les rues qui n'ont pas appartenu à cette pharma dans le passé (pblm : doivent aller trop loin)
            #if my_streets_idx[str_idx]['orig'] == off_idx:
            #    continue

            # recherche seulement dans les rues qui n'appartiennent pas encore à cette pharma
            if my_streets_idx[str_idx]['now'] == off_idx:
                continue

            # ne prendre qu'aux pharma qui ont plus de volume que soi
            if v0 > my_groups[my_streets_idx[str_idx]['now']]['n']:
                continue
            
            d2  = (officine['x'] - street['x'])*(officine['x'] - street['x']) + (officine['y'] - street['y'])*(officine['y'] - street['y']);
            if d2 < d20:
                d20 = d2
                t = street

        #t est la rue la plus proche
        str_idx = t['rue'] + '_' + t['cp']
        #print(str_idx)

        # cette rue doit être ajoutée à la pharma, ainsi que son nbre
        my_groups[off_idx]['done']         = True
        my_groups[off_idx]['n']            += t['n']
        my_groups[off_idx]['list'][str_idx] = t

        # et retirée de la pharma à laquelle elle a été prise
        old_off_idx = my_streets_idx[str_idx]['now']
        my_groups[old_off_idx]['n']                 -= t['n']
        del my_groups[old_off_idx]['list'][str_idx]

        #enfin, la nouvelle pharma doit remplacer l'ancienne dans la rue
        my_streets_idx[str_idx]['now'] = off_idx

        #print(my_groups[off_idx])

print('')
for k, v in my_groups.items():
    if v['n0'] != v['n']:
        print (v['name'] + ' ' + str(v['n0']) + '->' + str(v['n']))

###########################################
# end rebalancing
###########################################

off = []
out = [];
i   = 0;        # will be a readable index to be used in QGIS

for k,u in my_groups.items():
    off.append([u['id'], i, u['name'], u['descr'], u['n'], u['x'], u['y']])
    
    for l,t in u['list'].items():
        out.append( [t['x'], t['y'], t['n'], t['rue'], i, u['id'] ])

    i+=1

with open("off.csv", "w", encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(off)
    
with open("out.csv", "w", encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(out)

# these files can be loaded in QGIS
# out.csv should be joined with off.csv (join field : field_2, target field : field_5)
# this allows to visualize
#   - which officines are the most loaded (off.csv - using a category style based on the number of people) 
#   - which streets are bounded to each officine (out.csv - using a category style based on the index of the officine linked to a articular street)
#   - the out.csv layer can even be duplicated and styled using the geometry generator -> Line -> and the following expression : make_line( make_point( "field_1","field_2"),make_point( "officines [off.csv]_field_6","officines [off.csv]_field_7"))
#
# to be able to modify, the out.csv should be first exported in shapefile or geopackage                 

print(tabulate.tabulate(out))

