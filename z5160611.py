from flask import Flask, jsonify, make_response
from flask_restx import Api, Resource, reqparse
import requests
import json
import sqlite3

# ==== Important Variables ====
app = Flask(__name__)
api = Api(app)


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
    "show_name": show_object['show']['name'],
    "tvmaze_show_id" : show_object['show']['id'],
    "show_genre" : show_object['show']['genres'],
    "show_language" : show_object['show']['language'],
    "show_score" : show_object['score'],
    "show_links" : show_object['show']['_links']
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

# ====
@api.route('/tv_shows/<string:tv_show_name>')
class Tv_Show_Name(Resource):

  def get(self, tv_show_name):
    # fetch the data from the TV Maze API
    response = getTvShowName(tv_show_name)
    #print(type(response))
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
    isAlreadyInDb = checkIfEntryAlreadyExists(matching_show_with_id['tvmaze_show_id'], 'tvmaze_id')

    if(isAlreadyInDb == 1):
      # already exists
      data = {'message': 'TV show already exists in local database', 'code':'SUCCESS'}
      return make_response(jsonify(data), 204)
    else:
      # send the data to the database
      insertToDatabase(matching_show_with_id)

      # returns the response - should it be returned in json? I think yes.
      return matching_show_with_id

@api.route('/tv_shows/<int:id>')
class Retrieve_TV_Show(Resource):
  def get(self, id):
    print("this is the id: ", id)
    isExist = checkIfEntryAlreadyExists(id, 'id')
    print("isExist: ", isExist)
    if(not isExist):
      print("inside this not exists condition")
      # doesn't exist
      api.abort(404, "TV Show {} doesn't exist".format(id))
   
    print("before result")
   # return the entry 
    result = retrieveTvShowById(id)
    print("result type: ", type(result))
    return json.dumps(result)
    

# ==== Database functions ====
def insertToDatabase(x):
  try:
    sqliteConnection = sqlite3.connect('z5160611.db')
    cursor = sqliteConnection.cursor()
    print("Successfully connected to SQLite")

    cursor.execute("insert into TV_SHOWS_DATABASE (id, tvmaze_id, name, language, score) values (?, ?, ?, ?, ?)", 
                  (x['id'], x['tvmaze_show_id'], x['show_name'], x['show_language'], x['show_score']))
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
                  (id, tvmaze_id, name, language, score) """)
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
    print("row: ", row)
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
    print(row)
  except sqlite3.Error as error:
    print("Failed to execute testGetColumns: ", error)
  finally:
    if sqliteConnection:
      sqliteConnection.close()
      print("The SQL connection is clsed: from testGetColumns")


# ====
createTable()
testGetColumns()

if __name__ == '__main__':
  
  app.run(debug=True)
 



