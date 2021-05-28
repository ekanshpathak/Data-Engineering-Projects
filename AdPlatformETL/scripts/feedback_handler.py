import sys, uuid, time
from flask import Flask, request, abort
import mysql.connector as sql
from datetime import datetime
from kafka import KafkaProducer

app = Flask(__name__)
app.config["DEBUG"] = True
app.config['RESTFUL_JSON'] = {"ensure_ascii": False}

if len(sys.argv) != 7:
    print("Not enough parameters")
    exit(-1)

kafka_host = sys.argv[1]
kafka_topic = sys.argv[2]
database_host = sys.argv[3]
database_name = sys.argv[4]
database_username = sys.argv[5]
database_password = sys.argv[6]

try:
    db_conn = sql.connect(user=database_username, password=database_password,host=database_host, database=database_name)
except sql.Error as err:
    print(err)


#### Serve Ad API ####
@app.route('/ad/<ad_request_id>/feedback', methods=['POST'])

#### Main Function for the API ####
def UserFeedback(ad_request_id):
	feedback = request.json
	update_ad_feedback_time(ad_request_id)
	ad_related_data = get_ad_data(ad_request_id, feedback)

	publish_to_kafka(ad_related_data)

	remaining_budget = update_ads_budget(ad_related_data)

	if remaining_budget<=0:
		update_ad_status(ad_related_data)

	response = {"status": "SUCCESS"}
	#db_conn.close()
	
	return response
	
#### Function to Update TimeStamp in served_ads table ####
def update_ad_feedback_time(ad_request_id):
	db_cursor = db_conn.cursor()
	now = datetime.now()
	query = "update served_ads set timestamp= %s where request_id= %s"
	db_cursor.execute(query,(now,ad_request_id))
	db_conn.commit()
	db_cursor.close()

#### Function to collect Ad related data
def get_ad_data(ad_request_id, feedback):
	db_cursor = db_conn.cursor(dictionary=True)
	query = "select campaign_id, user_id, auction_cpm, auction_cpc, auction_cpa, target_age_range, target_location, target_gender, target_income_bucket, target_device_type, cast(campaign_start_time as char) as campaign_start_time, cast(campaign_end_time as char) as campaign_end_time, cast(timestamp as char) as timestamp from served_ads where request_id= %s"
	db_cursor.execute(query,(ad_request_id,))

	user_action = get_user_action(feedback)

	ad = {}
	ad_data = db_cursor.fetchall()
	for key in ad_data[0].keys():
		if key not in ad:
			ad[key] = ad_data[0][key]

	db_cursor.close()

	ad_feedback = collect_feedback_data(ad_request_id, feedback, user_action, ad)

	return ad_feedback

#### Derive Attributes like user_action ####
def get_user_action(feedback):
	if feedback["acquisition"]==1:
		user_action = "acquisition"
	elif feedback["click"]==1:
		user_action = "click"
	else:
		user_action = "view"

	return user_action

#### Collect all Ad related data to one Dict and create a Feedback JSON ####
def collect_feedback_data(ad_request_id, feedback, user_action, ad):
	ad_feedback_data = {}
	ad_feedback_data['request_id'] = ad_request_id

	for key in feedback.keys():
		if key not in ad_feedback_data.keys():
			ad_feedback_data[key] = feedback[key]

	for key in ad.keys():
		if key not in ad_feedback_data.keys():
			ad_feedback_data[key] = ad[key]

	if user_action == "acquisition":
		expenditure = ad['auction_cpa']
	elif user_action == "click":
		expenditure = ad['auction_cpc']
	else:
		expenditure = 0

	ad_feedback_data['action'] = user_action
	ad_feedback_data['expenditure'] = expenditure

	return ad_feedback_data

#### Publish the Ad Related Data to Kafka Topic ####
def publish_to_kafka(ad_related_data):
	bootstrap = [kafka_host]
	topic = kafka_topic
	producer = KafkaProducer(bootstrap_servers=bootstrap)

	message = bytes(str(ad_related_data), encoding='utf-8')

	ack = producer.send(topic, message)
	metadata = ack.get()

#### Update Budget of every Ad in ads table ####
def update_ads_budget(ad_related_data):
	campaign_id = ad_related_data['campaign_id']
	expenditure = ad_related_data['expenditure']

	db_cursor_select = db_conn.cursor(dictionary=True)

	get_budget_query = "select budget from ads where campaign_id= %s"
	db_cursor_select.execute(get_budget_query,(campaign_id,))

	for item in db_cursor_select:
		budget = item['budget']

	db_cursor_select.close()

	remaining_budget = budget - expenditure
	
	db_cursor_update = db_conn.cursor()

	update_budget_query = "update ads set budget= %s where campaign_id= %s"
	db_cursor_update.execute(update_budget_query,(remaining_budget,campaign_id))
	db_conn.commit()
	db_cursor_update.close()

	return remaining_budget

#### Update Ad Status ####
def update_ad_status(ad_related_data):
	campaign_id = ad_related_data['campaign_id']

	db_cursor_update = db_conn.cursor()

	update_status_query = "update ads set status='INACTIVE' where campaign_id= %s"
	db_cursor_update.execute(update_status_query,(campaign_id,))
	db_conn.commit()
	db_cursor_update.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)