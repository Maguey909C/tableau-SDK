from __future__ import print_function

import argparse
import sys
import textwrap
import os
from tableausdk import *
from tableausdk.HyperExtract import *


def getType(dType):
    """
    INPUT: Some type of data
    OUTPUT: A tabeau specific type name that will be used to populate df
    PURPOSE: To ensure data integrity based on the type of data input
    """
    # print ("DATE TYPE FROM getTYPE:", dType)
    if dType=='object':
        return Type.CHAR_STRING
    elif dType=='float64'or dType=='float32':
        return Type.DOUBLE
    elif dType=='int64' or dType=='int32':
        return Type.INTEGER
    elif dType=='bool':
        return Type.BOOLEAN
    elif dType=='datetime64[ns]' or dType=='datetime32[ns]':
        return Type.DATETIME
    else:
        return Type.CHAR_STRING

#   (Function assumes that the Tableau SDK Extract API is initialized)
def generateExtractFile(df, filename):
    """
    INPUT: A pandas dataframe, and the name of the tableau extract file you want
    OUTPUT: A tableau extract file
    PURPOSE: To generate a hypertext extract from a pandas dataframe
    """
    try:
        # Create Extract Object
        # (NOTE: The Extract constructor opens an existing extract with the
        #  given filename if one exists or creates a new extract with the given
        #  filename if one does not)
        extract = Extract( filename )

        # Define Table Schema (If we are creating a new extract)
        # (NOTE: In Tableau Data Engine, all tables must be named 'Extract')
        if ( not extract.hasTable( 'Extract' ) ):
            schema = TableDefinition()
            schema.setDefaultCollation( Collation.EN_US )
            for x in range(len(df.columns)):
                schema.addColumn(df.columns[x], getType(str(df.dtypes[x])))

            table = extract.addTable( 'Extract', schema )
            if ( table == None ):
                print('A fatal error occurred while creating the table:\nExiting now\n.')
                exit( -1 )

    except TableauException as e:
        print('A fatal error occurred while creating the new extract:\n', e, '\nExiting now.')
        exit( -1 )

    return extract


#   (NOTE: This function assumes that the Tableau SDK Extract API is initialized)
def populateExtract(df, extract):
    """
    INPUT: A dataframe, and the extract
    OUTPUT: None
    PURPOSE: To populate the extract based on the rows and column values in the pandas dataframe
    """

    try:
        # Get Schema
        table = extract.openTable( 'Extract' )
        schema = table.getTableDefinition()

        # Insert Data
        df = df.fillna(0)
        row = Row( schema )
        #for each row in the dataframe
        for y in range(df.shape[0]):
            table.insert( row )
            #for each column in the dataframe
            for x in range(len(df.columns)):
                # print ("COLUMN TYPE:", df.dtypes[x])
                # print ("XY type:", type(df[df.columns[x]][y]))
                # print ("XY value:", type(df[df.columns[x]][y]))
                if getType(df.dtypes[x])==Type.DOUBLE:
                    row.setDouble( x, df[df.columns[x]][y] )
                elif getType(df.dtypes[x])==Type.INTEGER:
                    row.setInteger( x, (df[df.columns[x]][y]))
                elif getType(df.dtypes[x])==Type.BOOLEAN:
                    row.setBoolean( x, df[df.columns[x]][y] )
                elif getType(df.dtypes[x])==Type.DATETIME:
                    row.setDateTime( x,
                                    df[df.columns[x]][y].year,
                                    df[df.columns[x]][y].month,
                                    df[df.columns[x]][y].day,
                                    df[df.columns[x]][y].hour,
                                    df[df.columns[x]][y].minute,
                                    df[df.columns[x]][y].second,
                                    0)
                else:
                    row.setCharString( x, str(df[df.columns[x]][y] ))

                # elif getType(df.dtypes[x])==Type.STRING:
                #     row.setString( x, df[df.columns[x]][y] )
                # elif getType(df.dtypes[x])==Type.DATE:
                #     row.setDate( x, df[df.columns[x]][y] )
                # elif getType(df.dtypes[x])==Type.SPATIAL:
                #     row.setSpatial( x, df[df.columns[x]][y] )
                # elif getType(df.dtypes[x])==Type.DURATION:
                #     row.setDuration( x, df[df.columns[x]][y] )



    except TableauException as e:
        print('A fatal error occurred while populating the extract:\n', e, '\nExiting now.')
        exit( -1 )

def createExtractFromDataFrame(df, filename):
    """
    INPUT: The dataframe you want to become a tableau extract file, and what you want that hyperextract file to be named
    OUTPUT: A tableau hyperextract file in your local directory
    PURPOSE: To generate a tableau hyperextract file from a pandas dataframe
    """

    if os.path.exists(filename):
      os.remove(filename)

    print ("File initializing")
    # Initialize the Tableau Extract API
    ExtractAPI.initialize()

    print ("Calling generate extract")
    # Create or Expand the Extract
    extract = generateExtractFile(df, filename)

    print ("\nPopulating extract")
    # Populate the extract
    populateExtract(df, extract)

    # Flush the Extract to Disk
    extract.close()

    # Close the Tableau Extract API
    ExtractAPI.cleanup()
