#!/usr/bin/python

# Base imports for all integrations, only remove these at your own risk!
import json
import sys
import os
import time
import pandas as pd
from collections import OrderedDict
import requests
from integration_core import Integration

from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from IPython.core.display import HTML

#import IPython.display
from IPython.display import display_html, display, Javascript, FileLink, FileLinks, Image
import ipywidgets as widgets
import jupyter_integrations_utility as jiu
# Put any additional imports specific to your integration here: 

from pyodbc_core._version import __desc__

import pyodbc as po

@magics_class  # Not sure about this, should pyodbc work by itself? Or should we 
class Pyodbc(Integration):
    # Static Variables
    # The name of the integration
    # The class name (Start) should be changed to match the name_str, but with the first letter upper cased.
    name_str = "pyodbc"
    instances = {}
    # These are the ENV variables the integration will check when starting up. The integration_base prefix will be prepended in checking (that defaults to JUPYTER_) 
    # So the following two items will look for:
    # JUPYTER_START_BASE_URL and put it into the opts dict as start_base_url
    # JUPYTER_START_USER as put it in the opts dict as start_user
    custom_evars = ["pyodbc_conn_default"]


    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    # The three examples here would be "start_base_url, start_ignore_ssl_warn, and start_verbose_errors
    # Make sure these are defined in myopts!
    custom_allowed_set_opts = ["pyodbc_conn_default"] 



    # These are the custom options for your integration    
    myopts = {} 
    myopts['pyodbc_max_rows'] = [1000, 'Max number of rows to return, will potentially add this to queries']
    myopts['pyodbc_conn_default'] = ["default", 'Default instance name for connections']
    
    # Class Init function - Obtain a reference to the get_ipython()

    def __init__(self, shell, debug=False, *args, **kwargs):
        super(Pyodbc, self).__init__(shell, debug=debug)
        self.debug = debug

        #Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]

        self.load_env(self.custom_evars)
        self.parse_instances()


    # We use a custom disconnect in pyodbc so we try to close the connection before nuking it
    def customDisconnect(self, instance):
        try:
            self.instances[instance]['connection'].close()
        except:
            pass
        self.instances[instance]['connection'] = None
        self.instances[instance]['session'] = None
        self.instances[instance]['connected'] = False
        #self.instances[instance]['connect_pass'] = None # Should we clear the password when we disconnect? I am going to change this to no for now 


    def req_password(self, instance):
        opts = None
        retval = True
        try:
            opts = self.instances[instance]['options']
        except:
            print("Instance %s options not found" % instance)
        try:
            if opts['use_integrated_security'] == 1:
                retval = False
        except:
            pass
        return retval

    def customAuth(self, instance):
        result = -1
        inst = None
        int_sec = False

        if instance not in self.instances.keys():
            print("Instance %s not found in instances - Connection Failed" % instance)
            result = -3
        else:
            inst = self.instances[instance]

        if inst is not None:

            try:
                if inst['options']['use_integrated_security'] == 1:
                    int_sec = True
            except:
                pass

            kar = [
                ["dsn", "DSN"], ["dbcname", "DBCNAME"], ["host", "Host"], ["port", "Port"],  ["default_db", "Database"], ["authmech", "AuthMech"], 
                ["usesasl", "UserSASL"],  ["user", "UID"], ["enc_pass", "PWD"], ["usessl", "SSL"],  ["allowselfsignedcert", "AllowSelfSignedServerCert"]
              ]


            top_level = ["user", "host", "port", "enc_pass"]
            var = []
            conn_vars = []
            for x in kar:
                if x[0] in top_level:
                    if int_sec == True and x[0] in ["user", "enc_pass"]: # No need to put UID and PWD in connect string
                        pass
                    else:
                        try:
                            tval = inst[x[0]]
                        except:
                            tval = None
                        tkey = x[1]
                        if x[0] == "enc_pass":
                            tval = self.ret_dec_pass(tval)
                            inst['connect_pass'] = ""
                else:
                    tval = self.checkvar(instance, x[0])
                    tkey = x[1]
                if tval is not None:    
                    conn_vars.append([tkey, tval])
            conn_string = ""
            for c in conn_vars:
                conn_string += "%s=%s; " % (c[0], c[1])
            conn_string = conn_string[0:-2]

            #conn_string = "DSN=%s; Host=%s; Port=%s; Database=%s; AuthMech=%s; UseSASL=%s; UID=%s; PWD=%s; SSL=%s; AllowSelfSignedServerCert=%s" % (var[0], var[1], var[2], var[3], var[4], var[5], var[6], var[7], var[8], var[9])

            try:
                self.instances[instance]['connection'] = po.connect(conn_string, autocommit=True)
                self.instances[instance]['session'] = self.instances[instance]['connection'].cursor()
#                inst['connection'] = po.connect(conn_string, autocommit=True)
#                inst['session'] = inst['connection'].cursor()
                result = 0
            except Exception as e:
                str_err = str(e)
                print("Unable to connect Error:\n%s" % str_err)
                result = -2

        # Here you can check if the authentication on connect is successful. If it's good, return 0, otherwise return something else and show an error

        return result

    def validateQuery(self, query, instance):


        bRun = True
        bReRun = False
        if self.instances[instance]['last_query'] == query:
            # If the validation allows rerun, that we are here:
            bReRun = True

        # Example Validation

        # Warn only - Don't change bRun
        # This one is looking for a ; in the query. We let it run, but we warn the user
        # Basically, we print a warning but don't change the bRun variable and the bReRun doesn't matter
        if query.find(";") >= 0:
            print("WARNING - Do not type a trailing semi colon on queries, your query will fail (like it probably did here)")

        # Warn and don't submit after first attempt - Second attempt go ahead and run
        # If the query doesn't have a day query, then maybe we want to WARN the user and not run the query.
        # However, if this is the second time in a row that the user has submitted the query, then they must want to run without day
        # So if bReRun is True, we allow bRun to stay true. This ensures the user to submit after warnings
        if query.lower().find("limit ") < 0:
            print("WARNING - Queries shoud have a limit so you don't bonkers your DOM")
        # Warn and do not allow submission
        # There is no way for a user to submit this query 
#        if query.lower().find('limit ") < 0:
#            print("ERROR - All queries must have a limit clause - Query will not submit without out")
#            bRun = False
        return bRun



    def customQuery(self, query, instance):
        mydf = None
        status = ""
        if self.debug:
            print(f"Instance: {instance}")
            print(f"Type of session: {type(self.instances[instance]['session'])}")

        try:
            self.instances[instance]['session'].execute(query)
            mydf = self.as_pandas_DataFrame(instance)
            if mydf is not None:
                status = "Success"
            else:
                status = "Success - No Results"
        except Exception as e:
            mydf = None
            str_err = str(e)
            if self.debug:
                print("Error: %s" % str(e))
            status = "Failure - query_error: " + str_err
        return mydf, status




# Display Help can be customized
    def customOldHelp(self):
        self.displayIntegrationHelp()
        self.displayQueryHelp("select * from mydatabase.mytable")


    def retCustomDesc(self):
        return __desc__


    def customHelp(self, curout):
        n = self.name_str
        mn = self.magic_name
        m = "%" + mn
        mq = "%" + m
        table_header = "| Magic | Description |\n"
        table_header += "| -------- | ----- |\n"
        out = curout
        qexamples = []
        qexamples.append(["myinstance", "select * from mydatabase.mytable", "Run a sql query against myinstance"])
        qexamples.append(["", "select * from mydatabase.mytable", "Run a sql query against the default instance"])
        out += self.retQueryHelp(qexamples)

        return out





    def as_pandas_DataFrame(self, instance):
        cursor = self.instances[instance]['session']
        try:
            names = [metadata[0] for metadata in cursor.description]
            ret =  pd.DataFrame([dict(zip(names, row)) for row in cursor], columns=names)
        except:
            ret = None
        return ret


    # This is the magic name.
    @line_cell_magic
    def pyodbc(self, line, cell=None):
        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)
            if self.debug:
                print("line: %s" % line)
                print("cell: %s" % cell)
            if not line_handled: # We based on this we can do custom things for integrations. 
                if line.lower() == "testintwin":
                    print("You've found the custom testint winning line magic!")
                else:
                    print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + " for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)


