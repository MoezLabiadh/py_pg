import os
from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement
import geopandas as gpd


def connect_to_pgsql (user, password, host, dbname):
    """ Connects to a PostgreSQL database using SQLAlchemy"""
    engine = create_engine(f'postgresql://{user}:{password}@{host}/{dbname}')
    
    return engine


def esri_to_gdf (aoi):
    """Returns a Geopandas file (gdf) based on 
       an ESRI format vector (shp or featureclass/gdb)"""
    
    if '.shp' in aoi: 
        gdf = gpd.read_file(aoi)
    
    elif '.gdb' in aoi:
        l = aoi.split ('.gdb')
        gdb = l[0] + '.gdb'
        fc = os.path.basename(aoi)
        gdf = gpd.read_file(filename= gdb, layer= fc)
        
    else:
        raise Exception ('Format not recognized. Please provide a shp or featureclass (gdb)!')
    
    return gdf


def gdf_to_postgis(engine, gdf, table_name):
    """ Writes a geodataframe to a pgsql/postgis table"""
    gdf.columns = gdf.columns.str.lower().str.replace(' ', '_')
    
    geo_col= gdf.geometry.name
    srid= gdf.crs.to_epsg()
    geom_type= gdf.geom_type[0].upper()
    
    gdf['geom'] = gdf[geo_col].apply(lambda x: WKTElement(x.wkt, srid= srid))

    gdf.drop(geo_col, 1, inplace=True)
    
    gdf.to_sql(name= table_name, 
              con= engine, 
              if_exists= 'replace', 
              index= False,
              dtype={'geom': Geometry(geom_type, srid= srid)})
    
    
if __name__ == '__main__':
    
    host= 'localhost'
    dbname= 'wc_data'
    user= os.getenv('pg_user')
    password= os.getenv('pg_pwd')

    engine= connect_to_pgsql(user, password, host, dbname)
    
    aoi= '20230929_waterApplics_KFN.shp'
    
    #read a spatial file into gdf
    gdf= esri_to_gdf (aoi)
    
    gdf_to_postgis(engine, gdf, '20230929_waterapplics_kfn')
