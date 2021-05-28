import sys, uuid
from flask import Flask, request, jsonify, abort
import mysql.connector as sql
import re

app = Flask(__name__)
#app.config["DEBUG"] = True
#app.config['RESTFUL_JSON'] = {"ensure_ascii": False}

if len(sys.argv) != 5:
    print("Not enough parameters")
    exit(-1)

database_host = sys.argv[1]
database_username = sys.argv[2]
database_password = sys.argv[3]
database_name = sys.argv[4]

try:
    db_conn = sql.connect(user=database_username, password=database_password,host=database_host, database=database_name)
except sql.Error as err:
    print(err)


#### Serve Ad API ####
@app.route('/ad/user/<user_id>/serve', methods=['GET'])

## Main Function for Ad Auction ##
def ad_auction(user_id):
    request_params = request.args

    user_data = get_user_data(user_id)

    top_2_ads = get_ads(user_data, request_params)

    winning_ad = find_winning_ad(top_2_ads)

    return winning_ad

## Function to get User related information like preferences, gender, age, etc ##
def get_user_data(user_id):
    if user_id=='1111-1111-1111-1111':
        user_info = {'user_id':user_id}
    else:
        db_cursor = db_conn.cursor(dictionary=True)
        user_query = "select * from users where id = %s"
        db_cursor.execute(user_query,(user_id,))

        for user in db_cursor:
            age, gender, income_bucket = user["age"], user["gender"], user["income_bucket"]
            
            websites, movies, music, program, books = DataCleaning(user["websites"],'websites'), DataCleaning(user["movies"],'movies'),DataCleaning(user["music"],'music'),DataCleaning(user["program"],'program'),DataCleaning(user["books"],'books')

            positives, negatives = DataCleaning(DataCleaning(eval(user["positives"]),'likes'),'likes2'), DataCleaning(DataCleaning(eval(user["negatives"]),'dislikes'),'dislikes2')
        
        db_cursor.close()

        categories_list = [websites, movies, music, program, books]
        category = [i for item in [item.split(' ') for item in [item.strip().replace('&amp;','&') for cat in categories_list for item in cat]] for i in item]
        
        try:
            while True:
                category.remove('&')
        except ValueError:
            pass

        likes = [i for item in [item.split(', ') if ',' in item else item.split(' ') for item in positives] for i in item]
        dislikes = [i for item in [item.split(', ') if ',' in item else item.split(' ') for item in negatives] for i in item]
        
        for cat in [category, likes, dislikes]:
            try:
                while True:
                    cat.remove('')
            except ValueError:
                pass

        user_info = {'user_id':user_id, 'age':age, 'gender':gender, 'income_bucket':income_bucket, 'category':category, 'likes':likes, 'dislikes':dislikes}
    return user_info

## Clean Likes & Dislikes Data, remove extra spaces & commas from the data ##
def DataCleaning(category,type):
    if type=='likes' or type=='dislikes':
        cats = []
        if len(category)==1 and len(category[0].split(','))>1:
            cat1 = category[0].split(',')
            for i in cat1:
                cats.append(i.strip())
        else:
            cats = category
    else:
        cat = str(category).split("'")[1:-1]
        cats = [re.sub('[^a-zA-Z0-9]+', '', i) for item in cat for i in item.split(' ')]

    return cats


## Function to get relevant ads on the basis of User Info & Query Parameters ##
def get_ads(user_data, request_params):
    db_cursor = db_conn.cursor(dictionary=True)

    user_id = user_data['user_id']
    device_type, state, city = request_params['device_type'], request_params['state'], request_params['city']

    if user_id=='1111-1111-1111-1111':
        ad_query = "select text, campaign_id, cpm, cpc, cpa, target_age_start, target_age_end, target_city, target_state, target_country, target_gender, target_income_bucket, target_device, cast(date_range_start as char) as date_range_start, cast(date_range_end as char) as date_range_end, cast(time_range_start as char) as time_range_start, cast(time_range_end as char) as time_range_end, rank() over (order by cpm desc) as 'rank' from ads where status='ACTIVE' and (target_age_start=0 and target_age_end=0) and target_gender='ALL' and target_income_bucket='ALL' and (target_device like '%{}%' or target_device='All') and target_city in ('All','{}') and target_state in ('All','{}') order by cpm desc limit 2".format(device_type[:6], city, state);
    else:
        user_age_start, user_age_end = user_data['age'], user_data['age']
        user_gender, user_income = user_data['gender'], user_data['income_bucket']
        user_preference, likes, dislikes = user_data['category'], user_data['likes'], user_data['dislikes']
        
        category_where_clause = "(1=1"
        for item in user_preference:
            category_where_clause += " or category like '%{}%'".format(item)
        category_where_clause += ")"

        likes_where_clause = "(1=1"
        for item in likes:
            likes_where_clause += " or keywords like '%{}%'".format(item)
        likes_where_clause += ")"

        dislikes_where_clause = "(1=1"
        for item in dislikes:
            dislikes_where_clause += " or keywords not like '%{}%'".format(item)
        dislikes_where_clause += ")"

        ad_query = "select text, campaign_id, cpm, cpc, cpa, target_age_start, target_age_end, target_city, target_state, target_country, target_gender, target_income_bucket, target_device, cast(date_range_start as char) as date_range_start, cast(date_range_end as char) as date_range_end, cast(time_range_start as char) as time_range_start, cast(time_range_end as char) as time_range_end, rank() over (order by cpm desc) as 'rank' from ads where status='ACTIVE' and (target_age_start<={} and target_age_end>={}) and target_gender in ('ALL', '{}') and target_income_bucket in ('ALL', '{}') and (target_device like '%{}%' or target_device='All') and target_city in ('ALL', '{}') and target_state in ('ALL', '{}') and {} and {} and {} order by cpm desc limit 2".format(user_age_start, user_age_end, user_gender, user_income, device_type[:6], city, state, category_where_clause, likes_where_clause, dislikes_where_clause);
        
    db_cursor.execute(ad_query)
    top_2_ads = db_cursor.fetchall()
    
    ads = {}
    for i in range(len(top_2_ads)):
        ads[i+1] = top_2_ads[i]
        ads[i+1]['user_id'] = user_id

    db_cursor.close()
    return ads

## Find Winning Ad among the two and Store in DB ##
def find_winning_ad(top_2_ads):
    if len(top_2_ads)==2:
        winning_ad = top_2_ads[1]
        second_best_ad = top_2_ads[2]
    else:
        winning_ad = top_2_ads[1]
        second_best_ad = top_2_ads[1]

    db_cursor = db_conn.cursor(dictionary=True)

    ad_text = winning_ad['text']
    request_id, campaign_id, user_id = str(uuid.uuid1()), winning_ad['campaign_id'], winning_ad['user_id']
    auction_cpm, auction_cpc, auction_cpa = second_best_ad['cpm'], second_best_ad['cpc'], second_best_ad['cpa']
    target_age_range = str(winning_ad['target_age_start'])+"-"+str(winning_ad['target_age_end'])
    target_location = winning_ad['target_city']+" "+winning_ad['target_state']+" "+winning_ad['target_country']
    target_gender, target_income_bucket, target_device_type = winning_ad['target_gender'], winning_ad['target_income_bucket'], winning_ad['target_device']
    campaign_start_time = winning_ad['date_range_start']+" "+winning_ad['time_range_start']
    campaign_end_time = winning_ad['date_range_end']+" "+winning_ad['time_range_end']

    best_ad_query = "INSERT INTO served_ads (request_id, campaign_id, user_id, auction_cpm, auction_cpc, auction_cpa, target_age_range, target_location, target_gender, target_income_bucket, target_device_type, campaign_start_time, campaign_end_time) VALUES ('{}','{}','{}',{},{},{},'{}','{}','{}','{}','{}','{}','{}')".format(request_id, campaign_id, user_id, auction_cpm, auction_cpc, auction_cpa, target_age_range, target_location, target_gender, target_income_bucket, target_device_type, campaign_start_time, campaign_end_time)

    db_cursor.execute(best_ad_query)
    db_conn.commit()
    db_cursor.close()

    best_ad = {'text':ad_text, 'request_id':request_id}

    return best_ad

if __name__ == "__main__":
    app.run(host="0.0.0.0")

