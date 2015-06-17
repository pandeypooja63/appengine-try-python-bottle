from operator import add
from bottle import Bottle
import MySQLdb
import logging
import os
import csv
import cloudstorage as gcs
from StringIO import StringIO
import time
from google.appengine.api import app_identity
from bottle import route,request,response,template


bottle = Bottle()

# Note: We don't need to call run() since our application is embedded within
# the App Engine WSGI application server.
# Retry can help overcome transient urlfetch or GCS issues, such as timeouts.
my_default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                          max_delay=5.0,
                                          backoff_factor=2,
                                          max_retry_period=15)
# All requests to GCS using the GCS client within current GAE request and
# current thread will use this retry params as default. If a default is not
# set via this mechanism, the library's built-in default will be used.
# Any GCS client function can also be given a more specific retry params
# that overrides the default.
# Note: the built-in default is good enough for most cases. We override
# retry_params here only for demo purposes.
gcs.set_default_retry_params(my_default_retry_params)

db = MySQLdb.connect(unix_socket='/cloudsql/icloud-971:sqldstore',user='root')
cur = db.cursor()


@bottle.route('/')
def hello():
    """Return a friendly HTTP greeting."""


    cur.execute("DROP DATABASE IF EXISTS earth")
    cur.execute("CREATE DATABASE IF NOT EXISTS earth")
    cur.execute("USE earth")
    cur.execute("DROP TABLE IF  EXISTS earthquake")
    cur.execute("CREATE TABLE IF NOT EXISTS earthquake( time timestamp ,latitude  double, longitude double,\
                 depth  double,mag    double,magType  varchar(100), nst   double,gap   double,dmin  double,\
                 rms double, net varchar(500),id varchar(80),updated  timestamp,place varchar(500),\
                 type VARCHAR(100))")
    cur.execute("TRUNCATE TABLE earthquake")
    bucket_name = os.environ.get('BUCKET_NAME',
                                 app_identity.get_default_gcs_bucket_name())
    bucket = '/' + bucket_name
    filename = bucket + '/all_month.csv'
    from StringIO import StringIO
    cr = read_file_insert(filename)
    cur.execute("(select week(time) ,count(*) as EQ ,mag from earthquake  where mag=2 or mag=3 or mag=4 or mag >=5 group by week(time),mag ORDER BY week(time))")
    data = cur.fetchall()
    d1=str(data)
    #print len(data)
    #print data
    #print ttime


    gt5 =0
    dict={}
    for x in data:
        if float(x[2])>=5.0:
            gt5 +=int(x[1])
            week=x[0]
            if dict.has_key(week):
               dict.update({week:gt5})
                #dict[week] =eval(dict[week]+gt5)
            else:
                dict[week]=gt5
                #dict(dict.items() + {week: gt5}.items())

        """ def insertIntoDataStruct(name,startTime,endTime,aDict):
         if not name in aDict:
        aDict[name] = [(startTime,endTime)]
         else:
        aDict[name].append((startTime,endTime))"""
        #display1=dict.keys()
        #display1.sort()
        #print dict
    final = ""
    for entry in dict:
        #final = final + entry,dict[entry]
        final = final +"<h3>"+ str(entry) +" &nbsp "+" >=5 "+" &nbsp "+ str(dict[entry])+"</h3>" +"<br> "

    display="Time taken ::"+str(cr)+"<br>"
    display = display+"<h3>"+ "Week"+"&nbsp"+"Magnitude"+"&nbsp"+" Number of Earthquakes"+"</h3>"+"<br>"
    for x in data:
        if x[2]>=5:
                continue
        else:
         display = display + "<h3> "+str(x[0])+"&nbsp&nbsp" + str(x[2]) +"&nbsp&nbsp"+str(x[1]) +" </h3>"+"<br>"

    return display + final



def read_file_insert( filename):
    gcs_file = gcs.open(filename)
    content= gcs_file.read()
    csv_reader = csv.reader(StringIO(content), delimiter=',', quotechar='"')
    starttime=time.time()
    for  row in csv_reader:
            if row[0]=="time":
             continue
            else :
             'true' if True else 'false'
             etime= row[0]
             newtime=etime.split("T")
             t1=newtime[0]
             t2=newtime[1].split(".")[0]
             ftime=t1+" "+t2
             utime= row[12]
             uptime=utime.split("T")
             u1=uptime[0]
             u2=uptime[1].split(".")[0]
             nutime=u1+" "+u2
             null_string = "''"
             query = "INSERT INTO earthquake values("
             query += "'"+ftime+"',"
             query += ""+row[1]+","
             query += ""+row[2]+","
             query += ""+row[3]+","
             query += ""+row[4]+","
             query += "'"+row[5]+"',"
             query += ""+row[6]+"," if row[6]!='' else null_string+","
             query += ""+row[7]+"," if row[7]!='' else null_string+","
             query += ""+row[8]+"," if row[8]!='' else null_string+","
             query += ""+row[9]+"," if row[9]!='' else null_string+","
             query += "'"+row[10]+"',"
             query += "'"+row[11]+"',"
             query += "'"+nutime+"',"
             query += "'"+row[13].replace("'","")+"',"
             query += "'"+row[14]+"'"
             query += ")"
             #print query
             cur.execute(query)


    endtime=time.time()
    ttime=endtime-starttime


    gcs_file.close()
    return ttime


@bottle.route('/calltemp')
def calltemp():
    return template('newfileupload')

#function to upload file on google bucket
@bottle.route('/uploadcsv',method='POST')
def uploadcsv():
    start_time = time.time()
    bucket_name = os.environ.get('BUCKET_NAME',app_identity.get_default_gcs_bucket_name())
    filename = "/"+bucket_name+"/all_months.csv"
    csvdata = request.files.get('mycsv')
    raw = csvdata.file.read()
    write_retry_params = gcs.RetryParams(backoff_factor=1.1)
    gcs_file = gcs.open(filename,'w',content_type='text/plain',options={'x-goog-meta-foo': 'foo','x-goog-meta-bar': 'bar'},retry_params=write_retry_params)
    gcs_file.write(raw)
    gcs_file.close()
    end_time = time.time()
    time_taken = end_time-start_time
    return template('upload',time_taken=time_taken)


"""@bottle.route('/calltemp')
def calltemp():
    return template('upload')

@bottle.route("/uploadcsv")
def uploadcsv():
    start_time = time.time()
    bucket_name = os.environ.get('BUCKET_NAME',app_identity.get_default_gcs_bucket_name())
    print bucket_name
    filename = '/'+bucket_name+ '/all_month.csv'
    csvupload = request.files.get('mycsv')
    csvdata = csvupload.file.read()
    write_retry_params = gcs.RetryParams(backoff_factor=1.1)
    gcs_file = gcs.open(filename,'w',content_type='text/plain',options={'x-goog-meta-foo': 'foo','x-goog-meta-bar': 'bar'},retry_params=write_retry_params)
    gcs_file.write(csvdata)
    gcs_file.close()
    end_time = time.time()
    time_taken = end_time-start_time
    print time_taken
    return str(time_taken)"""



    # Write the code necessary to determine the number of earthquakes for each
    #week, for magnitudes 2, 3, 4, and 5 (or greater)




# Define an handler for 404 errors.
@bottle.error(404)
def error_404(error):
    """Return a custom 404 error."""
    return 'Sorry, nothing at this URL.'


