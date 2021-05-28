from datetime import datetime
import sys, os
import mysql.connector as sql

if len(sys.argv) != 5:
    print("Not enough parameters")
    exit(-1)

database_host = sys.argv[1]
database_name = sys.argv[2]
database_username = sys.argv[3]
database_password = sys.argv[4]

try:
    db_conn = sql.connect(user=database_username, password=database_password,host=database_host, database=database_name)
except sql.Error as err:
    print(err)

#### Main Function ####
def main():
	remaining_slots = checkAndUpdateSlots()
	updateCurrentSlotBudget(remaining_slots)

#### Check and Update Remaining Slots for tracking purposes ####
def checkAndUpdateSlots():
	try:
		with open('/tmp/SlotsTracker.log','r') as slot_file:
			remaining_slots = int(slot_file.readline().split('=')[1])-1

		with open('/tmp/SlotsTracker.log','w') as updated_slot_file:
			updated_slot_file.write("slots={}".format(remaining_slots))

	except FileNotFoundError:
		with open('/tmp/SlotsTracker.log','w') as slot_file:
			slot_file.write("slots=5")
			remaining_slots = 5

	if remaining_slots>0:
		return remaining_slots
	else:
		print("No remaining Slots")
		exit(-1)

#### Identify the list of already served ads to update their Current Slot Budget ####
def updateCurrentSlotBudget(remaining_slots):
	cursor_fetch_ads = db_conn.cursor(buffered=True)
	cursor_fetch_budget = db_conn.cursor(buffered=True)
	cursor_update_slot_budget = db_conn.cursor(buffered=True)

	query_fetch_ads = "select distinct campaign_id from served_ads"
	query_fetch_budget = "select budget from ads where campaign_id=%s"
	query_update_slot_budget = "update ads set current_slot_budget=%s where campaign_id=%s"
	
	cursor_fetch_ads.execute(query_fetch_ads)

	for campaign_id in cursor_fetch_ads:
		cursor_fetch_budget.execute(query_fetch_budget,(campaign_id[0],))
		for budget in cursor_fetch_budget:
			updated_slot_budget = budget[0]/remaining_slots
			cursor_update_slot_budget.execute(query_update_slot_budget,(updated_slot_budget,campaign_id[0]))

			db_conn.commit()

if __name__ == '__main__':
	main()
else:
	print("Not a main file")

