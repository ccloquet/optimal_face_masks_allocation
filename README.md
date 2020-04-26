# optimal_face_masks_allocation
This code aims at allocating N face masks to M pharmacies so that each citizen can get one at its (almost) nearest pharmacy, while at the same time trying to avoid too large differences between pharmacies

Some comments in the code

Takes csv & geojson as input and creates csv as output.

The csv can be displayed in QGIS (sample project attached)

The streets are geocoded :  
 - first using the ICAR API of the Waloon Region
 - then if failure, using the Google Maps API
 - then if failure, uses a manually made table
 
Each street is represented as its center of mass (with all the associated limitations of this definition)
