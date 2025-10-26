import os
import json
import requests
import pandas as pd 
from mysql import connector
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_HOST = os.getenv("API_HOST")
SEASON = 2024
LEAGUE_ID=39

url = "https://api-football-v1.p.rapidapi.com/v3/standings"
headers = {
	"x-rapidapi-key": API_KEY,
	"x-rapidapi-host": API_HOST
}

querystring={"league":LEAGUE_ID,
             "season":SEASON}


response = requests.get(url=url, 
                        headers=headers,
                        params=querystring)
payload =response.json()

formatted_response = json.dumps(payload, indent=4)
standings_list=payload['response'][0]["league"]['standings'][0]

formatted_standings_list =json.dumps(standings_list, indent=4)
print(f"\n\n\n{formatted_standings_list}")

rows=[]
column_names=['season','position','team_id','team','played','won','draw','lost','goals_for','goals_against','goal_diff','points','form']

for club in standings_list:
    season=2024
    position= club['rank']
    team_id=club['team']['id']
    team=club['team']['name']
    played=club['all']['played']
    won=club['all']['win']
    draw=club['all']['draw']
    lost=club['all']['lose']
    goals_for=club['all']['goals']['for']
    goals_against=club['all']['goals']['against']
    goal_diff=club['goalsDiff']
    points=club['points']
    form=club['form']

    tuple_of_club_records= (season,position,team_id,team,played,won,draw,lost,goals_for,goals_against,goal_diff, points, form)

    rows.append(tuple_of_club_records)


df=pd.DataFrame(rows, columns=column_names)
print(df.head)


#LOAD

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

server_conn=connector.connect(
    
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    connection_timeout=10,
    autocommit=False,
    raise_on_warnings=True
)

server_cur= server_conn.cursor()
print("sucessfully connected!")

server_cur.close()
server_conn.close()

db_connection =connector.connect(
    
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE)

cur=db_connection.cursor()
print("successfully connected to database")

sql_table ="standings"
cur.execute("SHOW TABLES LIKE %s",(f"{sql_table}",))

if cur.fetchone() is None:
    raise SystemExit(f"This table'{sql_table}'is not found")
else:
    print("perfect! table exists")

