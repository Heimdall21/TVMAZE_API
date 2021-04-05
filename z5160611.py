from flask import Flask, jsonify, make_response, Response
from flask_restx import Api, Resource, reqparse
import requests
import json
import sqlite3
from datetime import datetime
import socket
import matplotlib.pyplot as plt
from collections import Counter
import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

# ==== Important Variables ====
app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('updateObject', type=json.loads)

parser1  = reqparse.RequestParser()
parser1.add_argument('order_by', type=str, default="+id")
parser1.add_argument('page', type=int, default=1)
parser1.add_argument('page_size', type=int, default=100)
parser1.add_argument('filter', type=str, default="id,name")

parser2 = reqparse.RequestParser()
parser2.add_argument('format', type=str)
parser2.add_argument('by', type=str)

# ==== Helper functions ====

def getTvShowName(x):
  response = requests.get('https://api.tvmaze.com/search/shows?q=' + x)
  if(response.status_code != 200):
    # something went wrong
    api.abort(response.status.code, "Error occurred during API request")
  return response

# this receives a list of objects, filters the list and returns it
def filterGetTvShowNameResponse(show_object):
  new_show_object = {
    "name": show_object['show']['name'],
    "tvmaze-id" : show_object['show']['id'],
    "language" : show_object['show']['language'],
    "rating" : json.dumps(show_object['show']['rating']),
    "_links" : json.dumps(show_object['show']['_links']),

    "last-updated": json.dumps(datetime.now(), indent=4, sort_keys=True, default=str),
    "type": show_object['show']['type'],
    "genre" : json.dumps(show_object['show']['genres']),
    "status": show_object['show']['status'],
    "runtime": show_object['show']['runtime'],
    "premiered": show_object['show']['premiered'],
    "official-site": show_object['show']['officialSite'],
    "schedule": json.dumps(show_object['show']['schedule']),
    "weight": show_object['show']['weight'],
    "network": json.dumps(show_object['show']['network']),
    "summary": show_object['show']['summary'],
  }
  return new_show_object

def addUniqueApiID(show_match):
  # get the latest row number
  row = countNoTvShowTableRows()
  if(row == 0):
    # then first id of new batch = 1
    id = 1
    show_match['id'] = id
  else:
    # there is an existing set of rows
    latest_id = getLatestRowId()
    show_match['id'] = latest_id + 1
  return show_match

def convertTupleToResponseObject(tuple):
  new_object = {
    "id": tuple[0],
    "tvmaze-id": tuple[1],
    "name": tuple[2],
    "language": tuple[3],
    "rating": tuple[4],
    "_links": tuple[5],
    "last-updated": tuple[6],
    "type": tuple[7],
    "genre": tuple[8],
    "status": tuple[9],
    "runtime": tuple[10],
    "premiered": tuple[11],
    "official-site": tuple[12],
    "schedule": tuple[13],
    "weight": tuple[14],
    "network": tuple[15],
    "summary": tuple[16],

  }
  return new_object

def updateRow(cleaned_result, updateObject):
  for k in updateObject.keys():
    newValue = updateObject[k]
    cleaned_result[k] = newValue
  cleaned_result['last_updated'] = json.dumps(datetime.now(), indent=4, sort_keys=True, default=str)
  return cleaned_result

def convertStringToArray(fooString):
  filterArray = fooString.split(',')
  length = len(filterArray)
  filterTuple = tuple(filterArray)
  
  sqlInput = "("
  i = 0
  while i < (length - 1):
    sqlInput = sqlInput + "?, "
    i += 1
  sqlInput = sqlInput + "?)"
  print(filterTuple)
  dataPack = (filterTuple, sqlInput)
  return dataPack

def queryStringCreator(order_by, filterTuple, sqlInput):
  # orderby = "+rating-average,+id"
  orderString = ""
  orderbyInstructions = order_by.split(',')
  for instruction in orderbyInstructions:
    orderAttribute = instruction[1:]
    filteredOrderAttribute = filterKeyForDatabase(orderAttribute)
    singleInstruction = ""
    sign = instruction[0]
    
    if(sign == '+'):
      singleInstruction = filteredOrderAttribute + " ASC"
    elif(sign == '-'):
      singleInstruction = filteredOrderAttribute + " DESC"
    orderString = orderString + singleInstruction + ","
  
  orderString = orderString[:-1]
  filteredString = ', '.join(filterTuple)
  
  queryString = 'SELECT ' + filteredString + ' FROM TV_SHOWS_DATABASE ORDER BY ' + orderString

  return queryString


def filterKeyForDatabase(instruction):
  print(instruction)
  if(instruction == 'rating-average'):
    return "rating"
  elif(instruction == 'id'):
    return "id"
  elif(instruction == 'tvmaze-id'):
    return "tvmaze_id"
  elif(instruction == 'name'):
    return "name"
  elif(instruction == 'language'):
    return "language"
  elif(instruction == 'last-update'):
    return "last_updated"
  elif(instruction == 'premiered'):
    return "premiered"
  elif(instruction == 'runtime'):
    return "runtime"
  # for extra
  elif(instruction == 'type'):
    return "type"
  elif(instruction == 'genres'):
    return "genres"
  elif(instruction == 'status'):
    return "status"
  elif(instruction == 'officialSite'):
    return "official_site"
  elif(instruction == 'schedule'):
    return "schedule"
  elif(instruction == 'weight'):
    return "weight"
  elif(instruction == 'network'):
    return "network"
  elif(instruction == 'summary'):
    return "summary"

def convertTupleToDict(listOfLists, filterTuple):
  listOfDicts = []
  filterList = list(filterTuple)
  for tvShowList in listOfLists:
    zippedList = zip(filterList, tvShowList)
    zippedList = list(zippedList)
    dictShow = dict(zippedList)

    listOfDicts.append(dictShow)
  return listOfDicts

def selectRightPage(tvShows, page, page_size):
  # do something
  noTvShows = len(tvShows)
  beginning = (page - 1) * page_size

  # if they are asking for a page that doesn't exist
  if (noTvShows < beginning):
    return {"message":"no such page"}
  # there should be some results 
  else:
    leftTvShows = noTvShows - (beginning + 1)
    if (leftTvShows >= page_size):
      slicedTvShows = tvShows[beginning:(beginning + page_size)]
    else:
      slicedTvShows = tvShows[beginning:]
  return slicedTvShows


def languageStatistics(languageColumn):
  # right now we get a list of lists so
  flat_list = [item for sublist in languageColumn for item in sublist]

  parsedData = Counter(flat_list)
  return parsedData

def renderStatistics(parsedData):
  # data to plot
  labels = []
  values = []

  for x, y in parsedData.items():
    labels.append(x)
    values.append(y)

  # Plot
  # plt.pie(values, labels=labels)
  # plt.show()
  fig, ax1 = plt.subplots()
  ax1.pie(values, labels=labels)

  return fig

def trimJson(matching_show_with_id):
  if(type(matching_show_with_id['_links']) != dict):
    matching_show_with_id['_links'] = json.loads(matching_show_with_id['_links'])
  matching_show_with_id['genre'] = json.loads(matching_show_with_id['genre'])
  matching_show_with_id['schedule'] = json.loads(matching_show_with_id['schedule'])
  matching_show_with_id['network'] = json.loads(matching_show_with_id['network'])
  matching_show_with_id['rating'] = json.loads(matching_show_with_id['rating'])
  matching_show_with_id['last-updated'] = json.loads(matching_show_with_id['last-updated'])
  return matching_show_with_id

def genresStatistics(genresColumn):
  flat_list = [json.loads(item) for sublist in genresColumn for item in sublist]
  parsedData = Counter(tuple(item) for item in flat_list)
  #parsedData = Counter(flat_list)
  return parsedData

def statusStatistics(statusColumn):
  flat_list = [item for sublist in statusColumn for item in sublist]
  parsedData = Counter(flat_list)
  return parsedData

def typeStatistics(typeColumn):
  flat_list = [item for sublist in typeColumn for item in sublist]
  parsedData = Counter(flat_list)
  return parsedData

# humbug
def countRecentlyUpdatedTvShows(lastUpdatedColumn):
  flat_list = [json.loads(item) for sublist in lastUpdatedColumn for item in sublist]
  count = 0
  currentTime = datetime.now()
  for date in flat_list:
    dateWithSecondsRounded = date.split('.', 1)[0]
    dateTimeObject = datetime.strptime(dateWithSecondsRounded, '%Y-%m-%d %H:%M:%S')
    delta = currentTime - dateTimeObject
    deltaSeconds = delta.total_seconds()
    if(deltaSeconds > (24*3600)):
      count += 1
  return count
# ====
@api.route('/tv_shows/<string:tv_show_name>')
class Tv_Show_Name(Resource):

  def get(self, tv_show_name):
    # fetch the data from the TV Maze API
    response = getTvShowName(tv_show_name)
    response = response.json()

    if(len(response) > 0):
      matching_show = response[0]
      matching_show = filterGetTvShowNameResponse(matching_show)

      # now add unique database ids 
      matching_show_with_id = addUniqueApiID(matching_show)

    else:
      # no matches
      return "no matches for given tv show name", 204

    # check whether the show already exists in the database
    isAlreadyInDb = checkIfEntryAlreadyExists(matching_show_with_id['tvmaze-id'], 'tvmaze_id')

    if(isAlreadyInDb == 1):
      # already exists
      data = {'message': 'TV show already exists in local database', 'code':'SUCCESS'}
      return make_response(jsonify(data), 204)
    else:
      # send the data to the database
      insertToDatabase(matching_show_with_id)

      # returns the response - should it be returned in json? I think yes.
      # make sure for certain attributes; they are loaded again so the response doesn't contain json strings 
      preparedResponse = trimJson(matching_show_with_id)

      return preparedResponse

@api.route('/tv_shows/<int:id>')
class Retrieve_TV_Show(Resource):
  def get(self, id):
    isExist = checkIfEntryAlreadyExists(id, 'id')
    if(not isExist):
      # doesn't exist
      api.abort(404, "TV Show {} doesn't exist".format(id))
   
    # return the entry 
    result = retrieveTvShowById(id)
    cleaned_result = convertTupleToResponseObject(result)

    # create dynamic links
    added_link_result = addLinks(cleaned_result)

    # return - but with modifcations to turn json strings into dicts
    preparedResponse = trimJson(added_link_result)

    return preparedResponse

@api.route('/tv-shows/<int:id>')
class Delete_TV_Show(Resource):
  def delete(self, id):
    # check if entry exists
    isExist = checkIfEntryAlreadyExists(id, 'id')
    if(isExist == 1):
      deleteRowById(id)
      return {"message": "The tv show with id {} was removed from the database".format(id), "id":id}
    else:
      return {"message": "The tv show with id {} does not exist in the database".format(id), "id":id}

@api.expect(parser)
@api.route('/tv-shows/<int:id> ')
class Update_TV_Show(Resource):
  def patch(self, id):
    # gets args
    args = parser.parse_args()
    updateObject = args['updateObject']
    
    # 1. Check if id exists
    isExist = checkIfEntryAlreadyExists(id, 'id')
    if(isExist == 0):
      return {"message": "The tv show with id {} does not exist in the database and therefore will not be updated".format(id), "id":id}
    # 2. Retrieve show by id
    row = retrieveTvShowById(id)
    cleaned_result = convertTupleToResponseObject(row)
    # 3. Iterate through keys of update object and create new row
    new_row = updateRow(cleaned_result, updateObject)

    # 4. Delete old row
    deleteRowById(id)
    # 5. Add new row
    insertToDatabase(new_row)

    updateResponseObject = {
      "id": id,
      "last-update": new_row['last_updated'],
      "_links": json.loads(new_row['_links'])
    }

    return updateResponseObject


@api.expect(parser1)
@api.route('/tv-shows/retrieve-list')
class Get_Tv_Show_By_Order(Resource):
  def get(self):
    args = parser1.parse_args()
    order_by = args['order_by']  # always going to be 2 values 
    page = args['page'] # always going to be one value
    page_size = args['page_size'] # always going to be one value
    filter = args['filter'] # could be multiple values

    # 1. convert the parameters into sql queries
    dataPack = convertStringToArray(filter)
    filterTuple = dataPack[0]
    sqlInput = dataPack[1]

    # 2. Make the query string
    queryString = queryStringCreator(order_by, filterTuple, sqlInput)
    
    # 3. query the database
    result = queryDatabase(queryString)
  
    # 4. Clean up the data to be presentable - a) Turn the list of lists into a list of dicts with 
    tvShows = convertTupleToDict(result, filterTuple)

    # select the right page
    slicedTvShows = selectRightPage(tvShows, page, page_size)


    # get the links

    currentHostname = socket.gethostname()
    currentPortnumber = 5000
    self_link = "htttp://{}:{}/tv-shows?order_by{}&page={}page_size=1000&filter={}".format(currentHostname, currentPortnumber, order_by, page, filter)
    next_link = "htttp://{}:{}/tv-shows?order_by{}&page={}page_size=1000&filter={}".format(currentHostname, currentPortnumber, order_by, page + 1, filter)
    
    # create final return object
    responseObject = {
      "page": page,
      "page_size": page_size,
      "tv-shows": slicedTvShows,
      "_links": {
        "self": self_link,
        "next": next_link
      }
    }
    return responseObject

@api.expect(parser2)
@api.route('/tv-shows/statistics')
class Get_Tv_Show_Statistics(Resource):
  def get(self):
    
    args = parser2.parse_args()
    byAttr = args['by']
    formatAttr = args['format']


    if(formatAttr == "image"):
      # 1. Get the data for each attribute - language, genres, status, type <- these can all be pie graphs
      if(byAttr == "language"):
        # a) Get the language column data
        languageColumn = getColumn("language")
        data = languageStatistics(languageColumn)
        figure = renderStatistics(data)

        output = io.BytesIO()
        FigureCanvas(figure).print_png(output)
        return Response(output.getvalue(), mimetype='image/png')
      elif(byAttr == "genres"):
        genresColumn = getColumn("genre")
        # parses the data
        parsedData = genresStatistics(genresColumn)
        # create the statistic as an image and return
        figure = renderStatistics(parsedData)
        output = io.BytesIO()
        FigureCanvas(figure).print_png(output)
        return Response(output.getvalue(), mimetype='image/png')
      elif(byAttr == "status"):
        statusColumn = getColumn("status")
        parsedData = statusStatistics(statusColumn)
        
        figure = renderStatistics(parsedData)
        output = io.BytesIO()
        FigureCanvas(figure).print_png(output)
        return Response(output.getvalue(), mimetype='image/png')

      else:
        typeColumn = getColumn("type")
        parsedData = typeStatistics(typeColumn)
        
        figure = renderStatistics(parsedData)
        output = io.BytesIO()
        FigureCanvas(figure).print_png(output)
        return Response(output.getvalue(), mimetype='image/png')
    else:
      responseObject = {}
      # format is in json
      # 1. Get the data for each attribute - language, genres, status, type <- these can all be pie graphs
      if(byAttr == "language"):
        languageColumn = getColumn("language")
        parsedData = languageStatistics(languageColumn)
      elif(byAttr == "genres"):
        genresColumn = getColumn("genre")
        parsedData = genresStatistics(genresColumn)
      elif(byAttr == "status"):
        statusColumn = getColumn("status")
        parsedData = statusStatistics(statusColumn)
      else:
        typeColumn = getColumn("type")
        parsedData = typeStatistics(typeColumn)
   
      # get the rest of the data
      # a) total number of tv shows
      totalTvShows = countNoTvShowTableRows()
      # humbug
      # b) shows updated in the last 24 hours
      lastUpdatedColumn = getColumn("last_updated")
      totalTvShowsRecentlyUpdated = countRecentlyUpdatedTvShows(lastUpdatedColumn)

      responseObject = {
        "total": totalTvShows,
        "total-updated": totalTvShowsRecentlyUpdated,
        "values": parsedData
      }
      return responseObject

# ==== Database functions ====
def insertToDatabase(x):
  try:
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    print("Successfully connected to SQLite")

    cursor.execute("insert into TV_SHOWS_DATABASE (id, tvmaze_id, name, language, rating, _links, last_updated, type, genre, status, runtime, premiered, official_site, schedule, weight, network, summary) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                  (x['id'], x['tvmaze-id'], x['name'], x['language'], x['rating'], x['_links'], x['last-updated'], x['type'], x['genre'], x['status'], x['runtime'], x['premiered'], x['official-site'], x['schedule'], x['weight'], x['network'], x['summary']))
    sqliteConnection.commit()
    cursor.close()
  except sqlite3.Error as error:
    print("Failed to insert data into sqlite table", error)
  finally:
    if sqliteConnection:
      sqliteConnection.close()
      print("The SQLite connection is closed")

def createTable():
  try:
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS TV_SHOWS_DATABASE 
                  (id, tvmaze_id, name, language, rating, _links, last_updated, type, genre, status, runtime, premiered, official_site, schedule, weight, network, summary) """)
    sqliteConnection.commit()
    cursor.close()
  except sqlite3.Error as error:
      print("Failed to insert data into sqlite table", error)
  finally:
      if sqliteConnection:
        sqliteConnection.close()
        print("The SQLite connection is closed")


def countNoTvShowTableRows():
  # returns the id number of the last row
  # if no rows exist return 1
  try:
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    cursor.execute("SELECT COUNT (*) from TV_SHOWS_DATABASE")
    sqliteConnection.commit()
    rows = cursor.fetchone()[0]
    cursor.close()
    return rows
  except sqlite3.Error as error:
    print("Failed to count number of rows", error)
  finally:
      if sqliteConnection:
        sqliteConnection.close()
        print("The SQLite connection is closed: from the countTvShowTableLatestID function")

def getLatestRowId():
  # return the latest row id
  try:
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    cursor.execute("SELECT * FROM TV_SHOWS_DATABASE ORDER BY id DESC LIMIT 1")
    sqliteConnection.commit()
    latest_row = cursor.fetchone()[0]
    cursor.close()
    return latest_row
  except sqlite3.Error as error:
    print("Failed to count number of rows", error)
  finally:
      if sqliteConnection:
        sqliteConnection.close()
        print("The SQLite connection is closed: from the countTvShowTableLatestID function")


def checkIfEntryAlreadyExists(x, id_type):
  try:
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    if(id_type == 'tvmaze_id'):
      cursor.execute("SELECT EXISTS(SELECT 1 FROM TV_SHOWS_DATABASE WHERE tvmaze_id = (?))", (x,))
    elif(id_type == 'id'):
      cursor.execute("SELECT EXISTS(SELECT 1 FROM TV_SHOWS_DATABASE WHERE id = (?))", (x,))
    sqliteConnection.commit()
    result = cursor.fetchone()[0]
    if not result:
      return 0
    else:
      return 1
  except sqlite3.Error as error:
    print("Failed to check if entry already exists: ", error)
  finally:
      if sqliteConnection:
        sqliteConnection.close()
        print("The SQLite connection is closed: from the checkIfEntryAlreadyExists function")


def retrieveTvShowById(id):
  try:
    print("inside the retrieveTvShowId function")
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    cursor.execute("SELECT * FROM TV_SHOWS_DATABASE WHERE id = (?)", (id,))
    sqliteConnection.commit()
    row = cursor.fetchone()
    return row
  except sqlite3.Error as error:
    print("Failed to retrieve TV show by id: ", error)
  finally:
    if sqliteConnection:
      sqliteConnection.close()
      print("The SQL connection is closed: from the retrieveTvShowByUd function")

def testGetColumns():
  try:
    print("inside the try block of testGetColumns")
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    cursor.execute("PRAGMA table_info(TV_SHOWS_DATABASE)")
    sqliteConnection.commit()
    row = cursor.fetchall()
  except sqlite3.Error as error:
    print("Failed to execute testGetColumns: ", error)
  finally:
    if sqliteConnection:
      sqliteConnection.close()
      print("The SQL connection is clsed: from testGetColumns")

def deleteRowById(id):
  try:
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    cursor.execute("delete from TV_SHOWS_DATABASE where id = (?)", (id,))
    sqliteConnection.commit()
    result = cursor.fetchall()
    return result
  except sqlite3.Error as error:
    print("Failed to delete by id: ", error)
  finally:
    if sqliteConnection:
      sqliteConnection.close()
      print("The SQL connection is closed: from deleteRowById")


# checks if there are links before or after and adds links appropriate
def addLinks(cleaned_result):
  # checks if the id before or after exists
  currentId = cleaned_result['id']
  links = cleaned_result['_links']
  links_dict = json.loads(links)
  self_link = links_dict['self']['href']


  # get the column value of ids
  try:
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    cursor.execute("select id from TV_SHOWS_DATABASE")
    column = cursor.fetchall()

    columnNoTuples = [i[0] for i in column]
    index = columnNoTuples.index(currentId)
    nthInList = index + 1
    noIds = len(columnNoTuples)

    # self link
    currentHostname = socket.gethostname()
    currentPortnumber = 5000
    id = currentId
    link_string = "htttp://{}:{}/tv-shows/{}".format(currentHostname, currentPortnumber, id)
    new_self_link = {
      "self": {
        "href": link_string
      }
    }

    if(noIds == 0):
      return {"message":"no rows to add links from"}
    elif(noIds == 1):
      # return current self
      cleaned_result['_links'] = new_self_link
      return cleaned_result
    elif(noIds == 2):
      # return only a prev or a next PLUS a self
      if(nthInList == 2):
        # get the value of the previous id 
        prevId = columnNoTuples[(index - 1)]
        link_string = "htttp://{}:{}/tv-shows/{}".format(currentHostname, currentPortnumber, prevId)
        previous_link = {
          "previous": {
            "href": link_string
          }
        }
        _links = {
          "self": new_self_link,
          "previous": previous_link
        }
        cleaned_result['_links'] = _links
        return cleaned_result
      else:
        nextId = columnNoTuples[(index + 1)]
        link_string = "htttp://{}:{}/tv-shows/{}".format(currentHostname, currentPortnumber, nextId)
        next_link = {
          "next": {
            "href": link_string
          }
        }
        _links = {
          "self": new_self_link,
          "next": next_link
        }
        cleaned_result['_links'] = _links
        return cleaned_result


    # this is if there are 3 or more rows
    # check if it's in the last row of the column
    if(nthInList == noIds):
      # then it's the last row id of a table with at least 3 rows
      prevId = columnNoTuples[(index - 1)]
      link_string = "htttp://{}:{}/tv-shows/{}".format(currentHostname, currentPortnumber, prevId)
      previous_link = {
        "previous": {
          "href": link_string
        }
      }
      _links = {
        "self": new_self_link,
        "previous": previous_link
      }
      cleaned_result['_links'] = _links
      return cleaned_result

    elif(nthInList == 1):
      # then it's the first row id in a table with at least 3 rows
      nextId = columnNoTuples[(index + 1)]
      link_string = "htttp://{}:{}/tv-shows/{}".format(currentHostname, currentPortnumber, nextId)
      next_link = {
        "next": {
          "href": link_string
        }
      }
      _links = {
        "self": new_self_link,
        "next": next_link
      }
      cleaned_result['_links'] = _links
      return cleaned_result
    else:
      # it's in the middle somewhere
      prevId = columnNoTuples[(index - 1)]
      link_string = "htttp://{}:{}/tv-shows/{}".format(currentHostname, currentPortnumber, prevId)
      previous_link = {
        "previous": {
          "href": link_string
        }
      }
      nextId = columnNoTuples[(index + 1)]
      link_string = "htttp://{}:{}/tv-shows/{}".format(currentHostname, currentPortnumber, nextId)
      next_link = {
        "next": {
          "href": link_string
        }
      }
      _links = {
        "self": new_self_link,
        "previous": previous_link,
        "next": next_link,
      }
      cleaned_result['_links'] = _links
      return cleaned_result

  except sqlite3.Error as error:
    print("Failed in the process of adding links: ", error)
  finally:
    if sqliteConnection:
      sqliteConnection.close()
      print("The SQL connection is closed from addLinks")

def queryDatabase(queryString):
  try:
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    cursor.execute(queryString)
    sqliteConnection.commit()
    result = cursor.fetchall()
    print("queryResult: ", result)
    return result
  except sqlite3.Error as error:
    print("Failed to query the database: ", error)
  finally:
    if sqliteConnection:
      sqliteConnection.close()
      print("The SQL connection is closed: from queryDatabase")

def getColumn(byAttribute):
  queryString = "SELECT " + byAttribute + " FROM TV_SHOWS_DATABASE"
  try:
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    cursor.execute(queryString)
    sqliteConnection.commit()
    result = cursor.fetchall()
    print("column result: ", result)
    return result
  except sqlite3.Error as error:
    print("Failed to query the database: ", error)
  finally:
    if sqliteConnection:
      sqliteConnection.close()
      print("The SQL connection is closed: from queryDatabase")



# ====
createTable()

if __name__ == '__main__':
  
  app.run(debug=True)
 



