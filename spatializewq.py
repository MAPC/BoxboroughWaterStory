# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 12:40:50 2020

@author: cspence
"""

import arcpy
from arcpy import env
from datetime import datetime
import numpy as np
import os
from os import path as p
import zipfile
import glob


mxd = arcpy.mapping.MapDocument("CURRENT")

''' 
Set up workspace 
'''

workspace = arcpy.GetParameterAsText(0)

arcpy.env.workspace = workspace
arcpy.env.qualifiedFieldNames = False


# Assemble data
wqresults = arcpy.GetParameterAsText(1)

mclfile = arcpy.GetParameterAsText(2)

pws_fc = arcpy.GetParameterAsText(3)

output_folder = arcpy.GetParameterAsText(4)

arcpy.env.overwriteOutput = True # Allows us to overwrite the main shapefile.

# Define functions

def AutoName(table): 
    # function that automatically names a feature class or raster
    # Adapted from MAPC's stormwater toolkit script at https://github.com/MAPC/stormwater-toolkit/blob/master/Burn_Raster_Script.py
    
    checktable = arcpy.Exists(table) # checks to see if the raster already exists
    count = 2
    newname = table

    while checktable == True: # if the raster already exists, adds a suffix to the end and checks again
        newname = table + str(count)
        count += 1
        checktable = arcpy.Exists(newname)

    return newname

    
def join_tables(table1, table1field, table2, table2field, outputname, method = 'KEEP_ALL'):
    # Function to join tables based on a common field, resulting in a hard-copy table with attributes from both
    
    shape_layer = AutoName(table2 + '_table')
    arcpy.MakeFeatureLayer_management(table2, shape_layer, workspace = workspace)
    
    temp_join = arcpy.AddJoin_management(shape_layer, table2field, table1, table1field, method)
    arcpy.CopyFeatures_management(temp_join, outputname)

    return(outputname)

# Start processing

# 1. Convert excel PWS water quality test file to table.
today = str(datetime.date(datetime.now()))
wqtable = 'wqtable_' + today.replace('-', '_')
arcpy.ExcelToTable_conversion(wqresults, wqtable, 'Results')

# 2. Convert excel maximum contaminant level/water quality guidelines to table.
mcltable = 'mcltable_' + today.replace('-', '_')
arcpy.ExcelToTable_conversion(mclfile, mcltable, 'Sheet1')

# Add numeric version of "Results"
arcpy.AddField_management(wqtable, 'result_mgL', 'DOUBLE')

# Calculate values as float with robustness to nulls.
row1 = 'Result'
row2 = 'result_mgL'
fields = [row1,row2]
with arcpy.da.UpdateCursor(wqtable, fields) as cursor:
    for row in cursor:
        try:
            row[1] = row[0]
            cursor.updateRow(row)
        except:
            row[1] = np.nan
            cursor.updateRow(row)
        
arcpy.DeleteField_management(wqtable, ['Result'])

# 3. Convert public water supplies geometry to text-only table.
pwstable = 'pwstable'
arcpy.TableToTable_conversion(pws_fc, workspace, pwstable)

# 4. Join public water supply table to water quality table
fcname1 = 'wqtestsint_' + today.replace('-', '_')
arcpy.AddMessage('Joining public water supply locations to test results. This could take awhile...')
join_tables(wqtable, 'PWS_ID', pws_fc, 'pws_id',  fcname1, method = 'KEEP_COMMON')

# 5. Join contaminant threshold to fc
fcname2 = 'wqtests_' + today.replace('-', '_')
arcpy.AddMessage('Joining water quality standards to test results...')
join_tables(mcltable, 'pwsname', fcname1, 'Chemical_Name', fcname2, method = 'KEEP_ALL')
arcpy.Delete_management(fcname1)

# 6. Calculate ratio between result and guideline.
arcpy.AddField_management(fcname2, 'ratio', 'DOUBLE')
exp = '!result_mgL!/!max2020!'
arcpy.CalculateField_management(fcname2, "ratio", exp, "PYTHON_9.3")

# 6. Export feature class to shapefile (to by zipped and uploaded)
arcpy.AddMessage('Exporting water quality feature class to shapefile...')
outname = 'wqtests'

arcpy.overwriteOutput = True
name = output_folder + "\\" + outname + ".shp"

arcpy.env.workspace = output_folder
if arcpy.Exists(name): arcpy.Delete_management(name)

arcpy.env.workspace = workspace

arcpy.FeatureClassToFeatureClass_conversion(fcname2, workspace, outname)

arcpy.FeatureClassToShapefile_conversion([outname], output_folder)
arcpy.AddMessage(output_folder)

# 5. Zip up the results.
arcpy.AddMessage('Zipping results...')




name = p.join(output_folder, outname + '.shp')
zip_path = p.join(output_folder, name + '.zip')
zip = zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED)

shp_zip_list = []

for f in glob.glob(name.replace(".shp",".*")):
    if not f.endswith(".lock"):
        if not f.endswith(".zip"):
            shp_zip_list.append(f)

for f in shp_zip_list:
    zip.write(f, os.path.basename(f))

zip.close()

# Delete unneeded files
arcpy.Delete_management(pwstable)
arcpy.Delete_management(wqtable)
arcpy.Delete_management(outname)



