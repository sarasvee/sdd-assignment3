from DataParser import DataParser
from DbConnector import DbConnector
from DBWriter import DBWriter

class Task1:
    def __init__(self):
        self.connection = DbConnector(DATABASE="my_db", HOST="localhost", USER="test_user", PASSWORD="test_password")
        self.client = self.connection.client
        self.db = self.connection.db
        self.db_writer = DBWriter(self.connection)
        self.parser = DataParser("dataset")
    
    def set_up_collections(self):
        self.create_coll("User")
        self.create_coll("Activity")
        self.create_coll("TrackPoint")
        
    def clear_db(self):
        print("Clearing DB")
        for coll_name in ["TrackPoint", "Activity", "User"]:
            self.drop_coll(coll_name)
        print("DB Cleared!")

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def parse_dataset_and_insert(self):

        print("parsing dataset...")
        users = self.parser.parse_users()

        # Insert users and activities
        print("Inserting users, activities and trackpoints...")
        filtered_users = filter(lambda u: len(u[1]) > 0, users)
        self.db_writer.insert_users_with_activities_trackpoints(filtered_users)

        print("Finished inserting data!")
    
    def read_activities_from_db(self):
        query = """SELECT id, user_id, start_date_time, end_date_time from %s"""
        self.cursor.execute(query % "Activity")
        rows = self.cursor.fetchall()
        return rows

if __name__ == "__main__":
    task = Task1()
    task.clear_db()
    task.set_up_collections()
    task.parse_dataset_and_insert()
    #task.read_activities_from_db()