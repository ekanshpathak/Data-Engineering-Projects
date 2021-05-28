import sys, decimal
from kafka import KafkaConsumer
import mysql.connector as sql
from json import loads

class KafkaToMySQL:
    def __init__(self, kafka_host, kafka_topic, database_host, database_name, database_username, database_password):

        #Initialize Kafka Consumer Client
        self.kafka_client = KafkaConsumer(kafka_topic,
                                     bootstrap_servers=kafka_host,
                                     auto_offset_reset='earliest',
                                     value_deserializer=lambda x: loads(x.decode('utf-8')))
        try:
            self.db_conn = sql.connect(user=database_username, password=database_password,
                              host=database_host, database=database_name)
        except sql.Error as err:
            print(err)

    def ReadKafka(self):
        try:
            for message in self.kafka_client:
                ad_data = message.value
                derived_attrs = self.DeriveAttributes(ad_data)
                self.InsertToMySQL(ad_data, derived_attrs)

        except KeyboardInterrupt:
            print("KeyboardInterrupt: Exiting Kafka Topic Reading")
            self.cursor.close()
            self.db_conn.close()
            sys.exit()
       
    def DeriveAttributes(self, ad):
        if ad['action'] in ['New Campaign','Update Campaign']:
            status = 'ACTIVE'
        else:
            status = 'INACTIVE'
            
        cpm = (0.0075 * float(ad['cpc'])) + (0.0005 * float(ad['cpa']))
                 
        #As per Guidelines, Slot Duration is 10mins and program will run for 1 hour. So, No. of Slots = 6
        slots = 6
        curr_slot_budget = ad['budget']/slots
        
        print("{} | {} | {}".format(ad['campaign_id'], ad['action'], status))

        return {'status' : status, 'cpm' : "{:.8f}".format(cpm), 'curr_slot_budget' : "{:.4f}".format(curr_slot_budget)}

    def InsertToMySQL(self, ad, derived_attrs):
        self.cursor = self.db_conn.cursor()
       
        if ad['action'] == 'New Campaign':
            query = "INSERT INTO ads (text, category, keywords, campaign_id, status, target_gender, target_age_start,target_age_end, target_city, target_state, target_country, target_income_bucket, target_device, cpc,cpa, cpm, budget, current_slot_budget, date_range_start, date_range_end, time_range_start, time_range_end) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data = (ad['text'],ad['category'],ad['keywords'],ad['campaign_id'],derived_attrs['status'],ad['target_gender'],ad['target_age_range']['start'],ad['target_age_range']['end'],ad['target_city'],ad['target_state'],ad['target_country'],ad['target_income_bucket'],ad['target_device'],ad['cpc'],ad['cpa'],derived_attrs['cpm'],ad['budget'],derived_attrs['curr_slot_budget'],ad['date_range']['start'],ad['date_range']['end'],ad['time_range']['start'],ad['time_range']['end'])

        elif ad['action'] == 'Update Campaign':
            query = "UPDATE ads set status='ACTIVE' where campaign_id=%s"
            data = (ad['campaign_id'],)

        else:
            query = "UPDATE ads set status='INACTIVE' where campaign_id=%s"
            data = (ad['campaign_id'],)

        self.cursor.execute(query, data)
        self.db_conn.commit()
        

if __name__ == "__main__":
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
        ad_manager_client = KafkaToMySQL(kafka_host, kafka_topic, database_host,
                                     database_name, database_username, database_password)
        ad_manager_client.ReadKafka()
        print("Reading the Topic ended...")


    except KeyboardInterrupt:
        print("Exiting Kafka Queue...")
        sys.exit()

