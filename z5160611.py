from flask import Flask
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
  return response

# this receives a list of objects, filters the list and returns it
def filterGetTvShowNameResponse(show_list):
  filtered_list = []
  for show_object in show_list:
    new_show_object = {
      "show_name": show_object['show']['name'],
      "tvmaze_show_id" : show_object['show']['id'],
      "show_genre" : show_object['show']['genres'],
      "show_language" : show_object['show']['language'],
      "show_score" : show_object['score'],
      "show_links" : show_object['show']['_links']
    }
    filtered_list.append(new_show_object)
  return filtered_list

def addUniqueApiID(show_list):
  # get the latest row number
  row = countNoTvShowTableRows()
  if(row == 0):
    # then first id of new batch = 1
    id = 1
    for x in show_list:
      x['id'] = id
      id += 1
  else:
    # there is an existing set of rows
    latest_id = getLatestRowId()
    for x in show_list:
      x['id'] = latest_id + 1
      latest_id += 1
  return show_list

# ====
@api.route('/tv_shows/<string:tv_show_name>')
class Tv_Show_Name(Resource):

  def get(self, tv_show_name):
    # fetch the data from the TV Maze API
    response = getTvShowName(tv_show_name)
    #print(type(response))
    response = response.json()
    # "response" is now a list, get the top 3 results
    filtered_list = []
    if(len(response) > 3):
      top_three_tvshows = response[:3]
      filtered_list = filterGetTvShowNameResponse(top_three_tvshows)
      # now add unique database ids 
      filtered_list_with_id = addUniqueApiID(filtered_list)
    else:
      filtered_list = filterGetTvShowNameResponse(top_three_tvshows)
      # now add unique database ids 
      filtered_list_with_id = addUniqueApiID(filtered_list)

    # send the data to the database
    insertToDatabase(filtered_list_with_id)

    # returns the response
    return filtered_list_with_id

# ==== Database functions ====
def insertToDatabase(show_list):
  for x in show_list:
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
    print("inside the createTable try block")
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
    cursor.execute("SELECT * FROM TV_SHOWS_DATABASE ORDER BY tvmaze_id DESC LIMIT 1")
    sqliteConnection.commit()
    latest_row = cursor.fetchone()[0]
    cursor.close()
    print("latest row: ", latest_row)
    return latest_row
  except sqlite3.Error as error:
    print("Failed to count number of rows", error)
  finally:
      if sqliteConnection:
        sqliteConnection.close()
        print("The SQLite connection is closed: from the countTvShowTableLatestID function")

# ====
print("inside main")
createTable()
print("after createTable")

if __name__ == '__main__':
  
  app.run(debug=True)
 



