# This is a sample Python script.
from pymongo import MongoClient


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    connection_string = "mongodb://admin:password@localhost:27017/?authMechanism=DEFAULT"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(connection_string)
    server_version = client.server_info()['version']
    print(f"Connected to MongoDB server version: {server_version}")

    # Close the MongoDB connection
    client.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
