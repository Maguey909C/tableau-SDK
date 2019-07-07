def publishHyperToServer(serverURL, site, project,  username, password, dataSourcePath):
    """
    INPUT: Sever URL, the site hosted, project name, your user name, credentials, and path to to the hypertext file you want published
    OUTPUT: A tableau hypertext file specified in the methods
    PURPOSE: Publishing a Tableau Hyper Extract File to tableau server

    EX: ('https://the/server/you/want.net',
                            'USA',
                            'ABC',
                            'username',
                            credentials,
                            "C://Users/CRENICK/Desktop/myfile.hyper)
    """

    import tableauserverclient as TSC
    import requests
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    from shutil import copyfile
    #copyfile(src, dst)
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    tableau_auth = TSC.TableauAuth(username, password, site_id=site)
    server = TSC.Server(serverURL)
    server.add_http_options({'verify': False})
    server.auth.sign_in(tableau_auth)
    targetProject=project
    #targetProjectID=''
#    dsPath = r'c://folder//hyptertext_file.hyper'
#    destPath=r"\\path\to\hypertext\file\stuff.hyper"
#    copyfile(dsPath, destPath)
    overwrite_true = TSC.Server.PublishMode.Overwrite
    with server.auth.sign_in(tableau_auth):
        all_projects, pagination_item = server.projects.get()
        all_DS, pagination_item = server.datasources.get()
        for proj in all_projects:
            if proj.name==targetProject:
                targetProjectID=proj.id
                new_datasource = TSC.DatasourceItem(targetProjectID)
                # publish data source (specified in file_path)
                #new_datasource.connections[0]
                new_datasource = server.datasources.publish(new_datasource, dataSourcePath, overwrite_true)

    server.auth.sign_out()
