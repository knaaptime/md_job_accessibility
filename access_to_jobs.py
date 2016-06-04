
import pandas as pd
import pandana as pdna
import geopandas as gpd


print "\nReading data"

# LEHD 2014 data (most recent available)
# download from
# http://lehd.ces.census.gov/data/lodes/LODES7/md/wac/md_wac_S000_JT01_2014.csv.gz
lehd = pd.read_csv('md_wac_S000_JT01_2014.csv')
lehd['geoid'] = lehd.w_geocode.astype('string')

# From LEHD Documentation:
# CD01  Number of jobs for workers with Educational Attainment: Less than high school
# CD02  Number of jobs for workers with Educational Attainment: High school or equivalent
# CD03  Number of jobs for workers with Educational Attainment: Some college or Associate degree
# CD04  Number of jobs for workers with Educational Attainment: Bachelor's degree or advanced degree

lehd['all_jobs'] = lehd.C000
lehd['low_skill'] = lehd.CD01 + lehd.CD02 # low skill = high school diploma or less
lehd['mid_skill'] = lehd.CD03
lehd['high_skill'] = lehd.CD04

lehd = lehd[['geoid', 'all_jobs', 'low_skill', 'mid_skill', 'high_skill']]


# Maryland census blocks from TIGER
# download from
# ftp://ftp2.census.gov/geo/tiger/TIGER2015/TABBLOCK/tl_2015_24_tabblock10.zip
blocks = gpd.GeoDataFrame.from_file('md_blocks_2010.shp')

gdf = blocks.merge(lehd, left_on='GEOID10', right_on='geoid', how="left")

# pandana only deals with point geometries, but the census blocks are polygons
# so we stash the old geometry column (because we want to export polys later)
# and use the geopandas centroid property to represent the blocks as points
gdf['geom_old']= gdf['geometry']
gdf['geometry'] = gdf['geometry'].centroid
gdf.head(5)

x, y = zip(*[(p.x, p.y) for (i, p)
             in gdf.geometry.iteritems()])
x = pd.Series(x)
y = pd.Series(y)

print "\nProcessing Network\n"

store = pd.HDFStore('osm_md.h5', "r")
nodes = store.nodes
edges = store.edges
print nodes.head(3)
print edges.head(3)

net=pdna.Network(nodes.x,
                       nodes.y,
                       edges["from"],
                       edges.to,
                       edges[["weight"]])
net.precompute(5000)
net.init_pois(num_categories=1, max_dist=8000, max_pois=1000)
net.set_pois("blocks", x, y)
node_ids = net.get_node_ids(x, y)


net.set(node_ids, variable=gdf.all_jobs, name="all_jobs")
net.set(node_ids, variable=gdf.low_skill, name="low_skill")
net.set(node_ids, variable=gdf.mid_skill, name="mid_skill")
net.set(node_ids, variable=gdf.high_skill, name="high_skill")

print "\nComputing accessibility"

# We will compute the total number of jobs within a 5 kilometer trip
# along the pedestrian network (read: no highways)
# probably longer than you'd want to walk, but reasonable for a bike ride
all_jobs = net.aggregate(5000, type="sum", decay="flat", name="all_jobs")
low_skill = net.aggregate(5000, type="sum", decay="flat", name="low_skill")
mid_skill = net.aggregate(5000, type="sum", decay="flat", name="mid_skill")
high_skill = net.aggregate(5000, type="sum", decay="flat", name="high_skill")

print "\nPreparing Output"

all_jobs.name = "all_jobs"
low_skill.name = 'low_skill'
mid_skill.name = 'mid_skill'
high_skill.name = 'high_skill'


all_jobs = pd.DataFrame(all_jobs)
low_skill = pd.DataFrame(low_skill)
mid_skill = pd.DataFrame(mid_skill)
high_skill = pd.DataFrame(high_skill)

access = all_jobs.join([low_skill, mid_skill, high_skill])

# set each column as an integer so we dont export a bunch of decimals
access.all_jobs = access.all_jobs.astype('int32')
access.low_skill = access.low_skill.astype('int32')
access.mid_skill = access.mid_skill.astype('int32')
access.high_skill = access.high_skill.astype('int32')

# drop the geometry column and reinstate the old one
# so that the census blocks return to polygons
gdf["node_ids"] = net.get_node_ids(x, y)
gdf['geometry'] = gdf['geom_old']
gdf.drop('geom_old', axis=1, inplace=True)

gdf = gdf[['geoid', 'geometry', 'node_ids']]

shape = gdf.merge(access, left_on="node_ids", right_index=True, how = "left")
# sometimes this needs to be reinitialized as a Geo df after a merge
shape = gpd.GeoDataFrame(shape)

print "\nWriting shapefile"

shape.to_file('output/access_to_jobs.shp')


print "\nComplete"
